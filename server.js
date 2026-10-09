const http=require('node:http'),net=require('node:net'),fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const mqtt=require('mqtt'),aedes=require('aedes'),PDFDocument=require('pdfkit');
const {Repository}=require('./repository');
const hash=(password,salt=crypto.randomBytes(16).toString('hex'))=>({salt,hash:crypto.scryptSync(password,salt,64).toString('hex')});
const verify=(p,u)=>{try{return crypto.timingSafeEqual(Buffer.from(u.hash,'hex'),crypto.scryptSync(p,u.salt,64));}catch{return false;}};
const uuid=()=>crypto.randomUUID(),now=()=>new Date().toISOString();
const ERROR=(message,status=400)=>Object.assign(new Error(message),{status});
const roles=['Administrador','Técnico','Consulta'];
const numeric=(v,min,max,name)=>{if(typeof v!=='number'||!Number.isFinite(v)||v<min||v>max)throw ERROR(`${name}: valor entre ${min} y ${max}.`);return v;};
const required=(v,name)=>{if(typeof v!=='string'||!v.trim()||v.length>200)throw ERROR(`${name} es obligatorio (máximo 200 caracteres).`);return v.trim();};
let state,repository,broker,brokerServer,publisher,subscriber,server,queue=[],busy=false;
const sessions=new Map(),attempts=new Map(),mqttSecret=crypto.randomBytes(32).toString('hex');
let live={mqtt:false,lastError:null};
function initial(){
 const adminPassword=process.env.ADMIN_PASSWORD||crypto.randomBytes(10).toString('base64url');
 console.log(`Primer acceso: admin / ${adminPassword}\nLa contraseña inicial se muestra solo en esta creación. Cámbiela en Usuarios.`);
 return {users:[{id:uuid(),username:'admin',name:'Edwin Castillo',role:'Administrador',active:true,...hash(adminPassword)}],areas:[{id:'A-01',name:'Informática'},{id:'A-02',name:'Iluminación'},{id:'A-03',name:'Servidores'}],points:[['PM-001','Equipos informáticos','A-01',8,120],['PM-002','Iluminación general','A-02',6,120],['PM-003','Cuarto de servidores','A-03',15,220]].map(([id,name,areaId,current,voltage])=>({id,name,areaId,baseCurrent:current,voltage,pf:0.95,interval:5,active:true,scenario:'normal',loss:0,delay:0,nextAt:0,seq:0,simEnergy:0,lastSample:null,received:0,expected:0})),rules:[{id:'R-001',pointId:'PM-001',variable:'current',operator:'>',limit:12,persistence:2,severity:'Alta',active:true}],measurements:[],events:[],reports:[],audit:[],transport:[],emissions:[],ruleState:{},settings:{running:false,staleIntervals:3,timezone:'America/Guatemala',tariff:1.5},createdAt:now()};
}
function audit(user,action){state.audit.push({id:uuid(),at:now(),user:user?.username||'Sistema',action});}
function ruleMatch(r,m){let v=m[r.variable];return r.operator==='>'?v>r.limit:v<r.limit;}
function ingest(raw){
 const m=JSON.parse(raw);const p=state.points.find(p=>p.id===m.pointId);if(!p||!p.active)throw ERROR('Punto desconocido o inactivo.');
 required(m.id,'Identificador');if(m.origin!=='simulated'||!Number.isInteger(m.seq)||m.seq<1||!Number.isFinite(Date.parse(m.at)))throw ERROR('Mensaje inválido.');
 for(const [key,min,max] of [['current',0,10000],['voltage',1,1000],['pf',0.01,1],['energyTotal',0,1e12],['energyDelta',0,1e9]])numeric(m[key],min,max,key);
 if(state.measurements.some(x=>x.id===m.id))return false;
 if(m.seq<= (p.lastSequence||0))throw ERROR('Mensaje fuera de orden.');
 const power=m.current*m.voltage*m.pf/1000;
 const outside=state.rules.filter(r=>r.active&&r.pointId===p.id&&ruleMatch(r,{...m,power})).map(r=>r.id);
 const row={...m,power,receivedAt:now(),outside,peak:outside.some(id=>state.rules.find(r=>r.id===id)?.operator==='>'),areaId:p.areaId,origin:'simulated'};
 state.measurements.push(row);p.received++;p.lastSequence=m.seq;p.lastReceived=row.receivedAt;
 for(const r of state.rules.filter(r=>r.active&&r.pointId===p.id)){
  const key=r.id;const rs=state.ruleState[key]||{count:0,eventId:null,lastSeq:null};
  // Un hueco de secuencia interrumpe la persistencia: no inventar continuidad.
  if((rs.lastSeq!==null&&m.seq!==rs.lastSeq+1)||(rs.lastAt&&Date.parse(m.at)-Date.parse(rs.lastAt)>m.interval*1500))rs.count=0;
  if(ruleMatch(r,row)){
   if(rs.count===0){rs.evidence=[];rs.firstAt=m.at;}rs.evidence=[...(rs.evidence||[]),row.id].slice(-r.persistence);rs.count++;
   if(rs.count>=r.persistence&&!rs.eventId){const ev={id:uuid(),pointId:p.id,ruleId:r.id,severity:r.severity,start:rs.firstAt,end:null,status:'Abierta',evidence:[...rs.evidence],history:[],variable:r.variable,limit:r.limit,operator:r.operator};state.events.push(ev);rs.eventId=ev.id;}
   else if(rs.eventId){const ev=state.events.find(e=>e.id===rs.eventId);if(ev)ev.evidence.push(row.id);}
  }else{
   rs.count=0;if(rs.eventId){const ev=state.events.find(e=>e.id===rs.eventId);if(ev)ev.end=m.at;rs.eventId=null;}
  }
  rs.lastSeq=m.seq;rs.lastAt=m.at;state.ruleState[key]=rs;
 }
 state.transport.push({id:uuid(),at:row.receivedAt,pointId:p.id,seq:m.seq,status:'Recibido',detail:'MQTT QoS 1 · mensaje único validado'});
 return true;
}
function sample(p,t){
 let c=p.baseCurrent*(0.9+Math.random()*0.2);
 if(p.scenario==='sobrecarga')c=p.baseCurrent*2.3;
 if(p.scenario==='sin_consumo')c=0;
 const power=c*p.voltage*p.pf/1000;
 const delta=p.lastSample===null?0:power*(t-p.lastSample)/3600000;
 p.lastSample=t;p.simEnergy+=delta;p.seq++;p.expected++;
 const m={id:`${p.id}-${p.seq}`,pointId:p.id,seq:p.seq,at:new Date(t).toISOString(),current:c,voltage:p.voltage,pf:p.pf,energyDelta:delta,energyTotal:p.simEnergy,origin:'simulated',interval:p.interval};
 state.emissions.push({pointId:p.id,at:m.at,id:m.id});
 if(p.scenario==='desconexion'||Math.random()*100<p.loss){state.transport.push({id:uuid(),at:now(),pointId:p.id,seq:p.seq,status:'Perdido',detail:p.scenario==='desconexion'?'Enlace simulado desconectado':'Pérdida configurada'});return;}
 queue.push({due:t+p.delay,message:m});
 if(p.delay)state.transport.push({id:uuid(),at:now(),pointId:p.id,seq:p.seq,status:'En cola',detail:`Latencia ${p.delay} ms`});
}
async function tick(){
 if(busy||!state)return;busy=true;
 try{
  const t=Date.now();
  if(state.settings.running)for(const p of state.points.filter(x=>x.active))if(t>=p.nextAt){sample(p,t);p.nextAt=t+p.interval*1000;}
  queue.sort((a,b)=>a.due-b.due||a.message.seq-b.message.seq);
  const due=queue.filter(x=>x.due<=t);queue=queue.filter(x=>x.due>t);
  for(const x of due){if(!publisher.connected){state.transport.push({id:uuid(),at:now(),pointId:x.message.pointId,status:'Error',detail:'Broker no disponible'});continue;}await new Promise((resolve,reject)=>publisher.publish(`pg2/mediciones/${x.message.pointId}`,JSON.stringify(x.message),{qos:1},e=>e?reject(e):resolve()));}
  // Esperar a que el consumidor procese el MQTT, guardar desde un único ciclo.
  await new Promise(r=>setTimeout(r,20));await repository.save(state);live.lastError=null;
 }catch(e){live.lastError=e.message;console.error(e.message);}finally{busy=false;}
}
function filtered(q){const from=q.get('from')?Date.parse(q.get('from')):-Infinity,to=q.get('to')?Date.parse(q.get('to')):Infinity;if(Number.isNaN(from)||Number.isNaN(to)||from>to)throw ERROR('Período inválido.');const point=q.get('point')||'',area=q.get('area')||'';return {from,to,point,area,rows:state.measurements.filter(m=>Date.parse(m.at)>=from&&Date.parse(m.at)<=to&&(!point||m.pointId===point)&&(!area||m.areaId===area))};}
const sum=a=>a.reduce((s,v)=>s+v,0);
function analysis(q){
 const f=filtered(q),rows=f.rows;const total=sum(rows.map(m=>m.energyDelta));const globalRows=state.measurements.filter(m=>Date.parse(m.at)>=f.from&&Date.parse(m.at)<=f.to);const whole=sum(globalRows.map(m=>m.energyDelta));
 const day=m=>new Intl.DateTimeFormat('en-CA',{timeZone:state.settings.timezone,year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(m.at));
 const daily={},byPoint={},hours={};for(const m of rows){daily[day(m)]=(daily[day(m)]||0)+m.energyDelta;byPoint[m.pointId]=(byPoint[m.pointId]||0)+m.energyDelta;let h=new Intl.DateTimeFormat('es-GT',{timeZone:state.settings.timezone,hour:'2-digit',hourCycle:'h23'}).format(new Date(m.at));hours[h]=(hours[h]||0)+m.energyDelta;}
 const events=state.events.filter(e=>(!f.point||e.pointId===f.point)&&(!f.area||state.points.find(p=>p.id===e.pointId)?.areaId===f.area)&&Date.parse(e.start)>=f.from&&Date.parse(e.start)<=f.to);const counts={};events.forEach(e=>counts[e.pointId]=(counts[e.pointId]||0)+1);
 const expected=state.emissions.filter(m=>Date.parse(m.at)>=f.from&&Date.parse(m.at)<=f.to&&(!f.point||m.pointId===f.point)&&(!f.area||state.points.find(p=>p.id===m.pointId)?.areaId===f.area)).length;
 const sortMax=o=>Object.entries(o).sort((a,b)=>b[1]-a[1])[0];
 let reduction=null,variation=null,trend=null;
 if(Number.isFinite(f.from)&&Number.isFinite(f.to)&&f.to>f.from){const span=f.to-f.from;const base=state.measurements.filter(m=>Date.parse(m.at)>=f.from-span&&Date.parse(m.at)<f.from&&(!f.point||m.pointId===f.point)&&(!f.area||m.areaId===f.area));const baseTotal=sum(base.map(m=>m.energyDelta));if(baseTotal>0&&rows.length){variation=(total-baseTotal)/baseTotal*100;reduction=-variation;trend=Math.abs(variation)<1?'Estable':variation>0?'Creciente':'Decreciente';}}
 const values=Object.values(daily),outside=rows.filter(m=>m.outside.length).length;const active=state.points.filter(p=>p.active&&p.lastReceived&&Date.now()-Date.parse(p.lastReceived)<=p.interval*state.settings.staleIntervals*1000&&(!f.point||p.id===f.point)&&(!f.area||p.areaId===f.area)).length;
 const defs=[['Consumo energético',rows.length?total:null,'kWh','Σ incrementos de energía recibidos'],['Reducción del consumo',reduction,'%','(Base − actual) / base × 100; período anterior de igual duración'],['Variación del consumo',variation,'%','(Actual − base) / base × 100'],['Consumo promedio diario',values.length?sum(values)/values.length:null,'kWh/día','Σ consumo diario / días con registros (pueden ser parciales)'],['Consumo máximo diario',values.length?Math.max(...values):null,'kWh','MAX(consumo diario registrado)'],['Consumo mínimo diario',values.length?Math.min(...values):null,'kWh','MIN(consumo diario registrado)'],['Consumo por punto',Object.entries(byPoint).map(([p,v])=>`${p}: ${v.toFixed(5)} kWh`).join(' · ')||null,'','Σ incrementos por punto'],['Participación en el total',whole>0?total/whole*100:null,'%','Consumo filtrado / total monitoreado del período × 100'],['Picos de consumo',rows.filter(m=>m.peak??m.outside.some(id=>state.rules.find(r=>r.id===id)?.operator==='>')).length,'lecturas','COUNT(lecturas que superan umbral); distinto de episodios'],['Mediciones fuera de umbral',rows.length?outside/rows.length*100:null,'%','Lecturas fuera / recibidas × 100'],['Anomalías detectadas',events.length,'eventos','COUNT(episodios que cumplen persistencia)'],['Punto con más anomalías',sortMax(counts)?.[0]||null,'','MAX(eventos agrupados por punto)'],['Horario de mayor consumo',sortMax(hours)?`${sortMax(hours)[0]}:00–${String((Number(sortMax(hours)[0])+1)%24).padStart(2,'0')}:00`:null,'','MAX(energía agrupada por hora local)'],['Tendencia del consumo',trend,'','Comparación con período anterior equivalente; estable ±1 %'],['Mediciones registradas',rows.length,'registros','COUNT(mensajes únicos recibidos)'],['Tasa de recepción',expected?rows.length/expected*100:null,'%','Recibidas / emisiones esperadas del período × 100'],['Dispositivos activos',active,'puntos','Comunicación vigente en este instante']];
 return {indicators:defs.map(([name,value,unit,formula],i)=>({id:i+1,name,value,unit,formula})),daily,byPoint,rows:rows.length,expected,energy:total,coverage:expected?rows.length/expected*100:null,period:{from:Number.isFinite(f.from)?new Date(f.from).toISOString():null,to:Number.isFinite(f.to)?new Date(f.to).toISOString():null,point:f.point,area:f.area},origin:'Datos simulados; energía de los intervalos recibidos. La pérdida de mensajes reduce la cobertura.'};
}
function publicUser(u){return {id:u.id,username:u.username,name:u.name,role:u.role,active:u.active};}
function snapshot(q,u){const a=analysis(q);const f=filtered(q);return {user:publicUser(u),areas:state.areas,points:state.points.map(p=>({...p,last:state.measurements.findLast(m=>m.pointId===p.id)||null,communication:!p.active?'Inactivo':!p.lastReceived?'Sin datos':Date.now()-Date.parse(p.lastReceived)>p.interval*state.settings.staleIntervals*1000?'Sin comunicación':'Vigente'})),measurements:f.rows.slice(-300).reverse(),events:state.events,rules:state.rules,reports:state.reports.map(({csv,pdf,...r})=>r),analysis:a,settings:u.role==='Administrador'?state.settings:null,users:u.role==='Administrador'?state.users.map(publicUser):[],audit:u.role==='Administrador'?state.audit.slice(-100).reverse():[],transport:u.role==='Administrador'?state.transport.slice(-100).reverse():[],health:{database:repository.mode,mqtt:live.mqtt,lastError:live.lastError,running:state.settings.running,queued:queue.length,received:state.measurements.length,expected:state.emissions.length},createdAt:state.createdAt};}
async function body(req){let s='';for await(const c of req){s+=c;if(s.length>100000)throw ERROR('Solicitud demasiado grande.',413);}try{return s?JSON.parse(s):{};}catch{throw ERROR('JSON inválido.');}}
function json(res,status,data){res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(data));}
function session(req){const cookie=(req.headers.cookie||'').split(';').map(x=>x.trim()).find(x=>x.startsWith('pg2_session='));const id=cookie?.slice(12);const s=sessions.get(id);if(!s||s.expires<Date.now()){sessions.delete(id);throw ERROR('Sesión vencida. Inicie sesión.',401);}const u=state.users.find(u=>u.id===s.userId);if(!u?.active)throw ERROR('Sesión no válida.',401);return {id,u};}
function allow(u,accepted=['Administrador']){if(!accepted.includes(u.role))throw ERROR('Permiso insuficiente.',403);}
async function mutate(req,url,u){
 const b=await body(req),p=url.pathname;
 if(p==='/api/areas'){
  allow(u);const name=required(b.name,'Nombre');const id=required(b.id,'Identificador');if(!/^[A-Za-z0-9_-]+$/.test(id))throw ERROR('Identificador inválido.');const existing=state.areas.find(a=>a.id===id);if(existing&&!b.edit)throw ERROR('Identificador ya registrado.');if(existing)existing.name=name;else state.areas.push({id,name});audit(u,`Área guardada: ${id}`);
 }else if(p==='/api/points'){
  allow(u);const id=required(b.id,'Identificador'),name=required(b.name,'Nombre');if(!/^[A-Za-z0-9_-]+$/.test(id))throw ERROR('Identificador inválido.');if(!state.areas.some(a=>a.id===b.areaId))throw ERROR('Seleccione un área.');numeric(b.interval,1,3600,'Intervalo');numeric(b.baseCurrent,0,1000,'Corriente');numeric(b.voltage,1,1000,'Voltaje');numeric(b.pf,0.01,1,'Factor de potencia');
  const existing=state.points.find(x=>x.id===id);if(existing&&!b.edit)throw ERROR('Identificador ya registrado.');const values={id,name,areaId:b.areaId,interval:b.interval,baseCurrent:b.baseCurrent,voltage:b.voltage,pf:b.pf,active:!!b.active};
  if(existing){Object.assign(existing,values,{nextAt:0,lastSample:null});queue=queue.filter(x=>x.message.pointId!==id);for(const r of state.rules.filter(r=>r.pointId===id)){const rs=state.ruleState[r.id];if(rs?.eventId){const e=state.events.find(e=>e.id===rs.eventId);if(e)e.end=now();}delete state.ruleState[r.id];}}
  else state.points.push({...values,scenario:'normal',loss:0,delay:0,seq:0,simEnergy:0,nextAt:0,lastSample:null,expected:0,received:0});audit(u,`Punto guardado: ${id}`);
 }else if(p==='/api/rules'){
  allow(u);if(!state.points.some(p=>p.id===b.pointId))throw ERROR('Punto inválido.');if(!['current','power','voltage'].includes(b.variable)||!['>','<'].includes(b.operator)||!['Alta','Media','Baja'].includes(b.severity))throw ERROR('Condición inválida.');numeric(b.limit,0,100000,'Límite');numeric(b.persistence,1,1000,'Persistencia');if(!Number.isInteger(b.persistence))throw ERROR('Persistencia debe ser entera.');const id=b.id||uuid(),r=state.rules.find(r=>r.id===id);if(r){const rs=state.ruleState[id];if(rs?.eventId){const e=state.events.find(e=>e.id===rs.eventId);if(e)e.end=now();}delete state.ruleState[id];}const value={id,pointId:b.pointId,variable:b.variable,operator:b.operator,limit:b.limit,persistence:b.persistence,severity:b.severity,active:!!b.active};if(r)Object.assign(r,value);else state.rules.push(value);audit(u,`Regla guardada: ${id}`);
 }else if(p==='/api/events'){
  allow(u);const e=state.events.find(e=>e.id===b.id);if(!e)throw ERROR('Evento no encontrado.',404);if(!['En revisión','Cerrada'].includes(b.status))throw ERROR('Estado inválido.');if(e.status==='Cerrada')throw ERROR('Evento ya cerrado.');if(b.status==='Cerrada'&&!e.end)throw ERROR('La condición sigue activa. Recupere el escenario antes de cerrar.');const note=required(b.note,'Observación');e.status=b.status;e.history.push({at:now(),user:u.username,note,status:b.status});audit(u,`Evento ${e.id}: ${b.status}`);
 }else if(p==='/api/users'){
  allow(u);const name=required(b.name,'Nombre'),username=required(b.username,'Usuario');if(!roles.includes(b.role))throw ERROR('Grupo inválido.');if(state.users.some(x=>x.username.toLowerCase()===username.toLowerCase()&&x.id!==b.id))throw ERROR('Usuario ya registrado.');let existing=state.users.find(x=>x.id===b.id);if(!existing&&!b.password)throw ERROR('Contraseña obligatoria.');if(b.password&&b.password.length<10)throw ERROR('Contraseña mínimo 10 caracteres.');if(existing?.role==='Administrador'&&existing.active&&(!b.active||b.role!=='Administrador')&&state.users.filter(x=>x.role==='Administrador'&&x.active).length===1)throw ERROR('Debe conservar un administrador activo.');const v={name,username,role:b.role,active:!!b.active};if(b.password)Object.assign(v,hash(b.password));if(existing)Object.assign(existing,v);else state.users.push({id:uuid(),...v});if(existing)for(const [k,s]of sessions)if(s.userId===existing.id)sessions.delete(k);audit(u,`Cuenta guardada: ${username}`);
 }else if(p==='/api/settings'){
  allow(u);numeric(b.staleIntervals,1,100,'Vigencia');numeric(b.tariff,0,100,'Tarifa');try{new Intl.DateTimeFormat('es',{timeZone:b.timezone});}catch{throw ERROR('Zona horaria inválida.');}Object.assign(state.settings,{staleIntervals:b.staleIntervals,tariff:b.tariff,timezone:b.timezone});audit(u,'Configuración actualizada');
 }else if(p==='/api/simulation'){
  allow(u);
  if(typeof b.running==='boolean'){state.settings.running=b.running;for(const p of state.points){p.lastSample=null;p.nextAt=0;}audit(u,`Simulación ${b.running?'iniciada':'pausada'}`);}
  else {const point=state.points.find(x=>x.id===b.pointId);if(!point)throw ERROR('Punto inválido.');if(!['normal','sobrecarga','sin_consumo','desconexion'].includes(b.scenario))throw ERROR('Escenario inválido.');numeric(b.loss,0,100,'Pérdida');numeric(b.delay,0,60000,'Latencia');Object.assign(point,{scenario:b.scenario,loss:b.loss,delay:b.delay});audit(u,`Escenario ${b.scenario}: ${point.id}`);}
 }else if(p==='/api/reports'){
  allow(u,['Administrador','Técnico']);const a=analysis(url.searchParams),rows=filtered(url.searchParams).rows;if(!rows.length)throw ERROR('No hay mediciones para el período.');const title=required(b.title,'Título'),id=uuid(),at=now();
  const esc=x=>`"${String(x).replaceAll('"','""')}"`;const csv='\uFEFF'+[['Informe',title],['Generado',at],['Origen','SIMULADO'],['Filtros',JSON.stringify(a.period)],['Cobertura (%)',a.coverage??'No calculable'],['Fecha','Punto','Corriente A','Voltaje V','FP','Potencia kW','Energía intervalo kWh','Origen'],...rows.map(m=>[m.at,m.pointId,m.current,m.voltage,m.pf,m.power,m.energyDelta,'SIMULADO'])].map(r=>r.map(esc).join(',')).join('\r\n');
  const doc=new PDFDocument({size:'A4',margin:48}),chunks=[];doc.on('data',c=>chunks.push(c));const result=new Promise(resolve=>doc.on('end',()=>resolve(Buffer.concat(chunks).toString('base64'))));doc.fontSize(20).text(title);doc.moveDown().fontSize(10).text(`Sistema de monitoreo energético · PG2\nOrigen: SIMULADO\nGenerado: ${at}\nAutor: ${u.username}\nPeríodo: ${a.period.from||'Inicio'} a ${a.period.to||'Actual'}\nPunto: ${a.period.point||'Todos'} · Área: ${a.period.area||'Todas'}\nCobertura: ${a.coverage===null?'No calculable':a.coverage.toFixed(2)+' %'}\n${a.origin}`);doc.moveDown();for(const i of a.indicators){doc.fontSize(11).text(`${String(i.id).padStart(2,'0')} ${i.name}: ${i.value===null?'No calculable':typeof i.value==='number'?i.value.toFixed(5):i.value} ${i.unit}`);doc.fontSize(8).fillColor('#567080').text(i.formula).fillColor('#111111').moveDown(0.5);}doc.end();const pdf=await result;state.reports.push({id,title,at,author:u.username,period:a.period,coverage:a.coverage,origin:'SIMULADO',csv,pdf});audit(u,`Informe generado: ${title}`);
 }else throw ERROR('Ruta desconocida.',404);
 await repository.save(state);return {ok:true};
}
async function handler(req,res){
 try{
  res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('X-Frame-Options','DENY');res.setHeader('Content-Security-Policy',"default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; script-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'");
  const url=new URL(req.url,'http://localhost');
  if(url.pathname==='/api/login'&&req.method==='POST'){
   const b=await body(req),ip=req.socket.remoteAddress,limit=attempts.get(ip);if(limit&&limit.until>Date.now()&&limit.count>=10)throw ERROR('Demasiados intentos. Espere cinco minutos.',429);
   const u=state.users.find(x=>x.username===b.username);if(!u?.active||!verify(b.password,u)){attempts.set(ip,{count:(limit?.until>Date.now()?limit.count:0)+1,until:Date.now()+300000});throw ERROR('Credenciales inválidas o cuenta no disponible.',401);}attempts.delete(ip);const id=crypto.randomBytes(32).toString('hex');sessions.set(id,{userId:u.id,expires:Date.now()+3600000});res.setHeader('Set-Cookie',`pg2_session=${id}; HttpOnly; SameSite=Strict; Path=/; Max-Age=3600${process.env.COOKIE_SECURE==='true'?'; Secure':''}`);audit(u,'Inicio de sesión');return json(res,200,{user:publicUser(u)});
  }
  if(url.pathname.startsWith('/api/')){
   const {u,id}=session(req);
   if(req.method==='POST'&&req.headers.origin&&req.headers.origin!==`http://${req.headers.host}`&&req.headers.origin!==`https://${req.headers.host}`)throw ERROR('Origen inválido.',403);
   if(url.pathname==='/api/logout'){sessions.delete(id);res.setHeader('Set-Cookie','pg2_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0');return json(res,200,{ok:true});}
   if(req.method==='GET'&&url.pathname==='/api/state')return json(res,200,snapshot(url.searchParams,u));
   if(req.method==='GET'&&url.pathname.startsWith('/api/reports/')){const [, , ,rid,format]=url.pathname.split('/');const r=state.reports.find(r=>r.id===rid);if(!r||!['csv','pdf'].includes(format))throw ERROR('Informe no encontrado.',404);res.writeHead(200,{'Content-Type':format==='pdf'?'application/pdf':'text/csv; charset=utf-8','Content-Disposition':`attachment; filename="informe-${rid}.${format}"`});return res.end(format==='pdf'?Buffer.from(r.pdf,'base64'):r.csv);}
   if(req.method==='POST')return json(res,200,await mutate(req,url,u));throw ERROR('Ruta desconocida.',404);
  }
  if(req.method!=='GET')throw ERROR('Método no permitido.',405);
  const publicDir=path.join(__dirname,'public');const file=path.resolve(publicDir,'.'+decodeURIComponent(url.pathname==='/'?'/index.html':url.pathname));if(!file.startsWith(publicDir+path.sep))throw ERROR('Ruta inválida.',403);if(!fs.existsSync(file))throw ERROR('No encontrado.',404);const type={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'}[path.extname(file)]||'application/octet-stream';res.writeHead(200,{'Content-Type':type});fs.createReadStream(file).pipe(res);
 }catch(e){json(res,e.status||500,{error:e.status?e.message:'Error del servidor. Revise el diagnóstico.'});if(!e.status)console.error(e);}
}
async function main(){
 repository=new Repository();await repository.init();state=await repository.load()||initial();state.settings.running=false;for(const p of state.points){p.lastSample=null;p.nextAt=0;}
 broker=aedes();broker.authenticate=(client,user,password,done)=>done(null,user==='pg2'&&password?.toString()===mqttSecret);
 brokerServer=net.createServer(broker.handle);await new Promise(r=>brokerServer.listen(Number(process.env.MQTT_PORT||18883),'127.0.0.1',r));
 const opts={username:'pg2',password:mqttSecret};const address=`mqtt://127.0.0.1:${process.env.MQTT_PORT||18883}`;subscriber=mqtt.connect(address,{...opts,clientId:'pg2-backend'});publisher=mqtt.connect(address,{...opts,clientId:'pg2-simulador'});
 subscriber.on('message',(_topic,message)=>{try{ingest(message.toString());}catch(e){state.transport.push({id:uuid(),at:now(),status:'Rechazado',detail:e.message});}});
 await Promise.all([new Promise(r=>publisher.on('connect',r)),new Promise(r=>subscriber.on('connect',()=>subscriber.subscribe('pg2/mediciones/+',{qos:1},r)))]);live.mqtt=true;
 for(const c of [publisher,subscriber]){c.on('error',e=>live.lastError=e.message);c.on('offline',()=>live.mqtt=false);c.on('connect',()=>live.mqtt=publisher.connected&&subscriber.connected);}
 await repository.save(state);server=http.createServer(handler);await new Promise(r=>server.listen(Number(process.env.PORT||3000),process.env.HOST||'127.0.0.1',r));console.log(`Programa en ejecución: http://localhost:${process.env.PORT||3000}\nBase de datos: ${repository.mode}. MQTT local autenticado activo.`);
 const timer=setInterval(tick,250);
 const stop=async()=>{clearInterval(timer);while(busy)await new Promise(r=>setTimeout(r,25));state.settings.running=false;await repository.save(state);await new Promise(r=>publisher.end(false,r));await new Promise(r=>subscriber.end(false,r));brokerServer.close();broker.close();server.close();await repository.close();process.exit(0);};process.on('SIGINT',stop);process.on('SIGTERM',stop);
}
if(require.main===module)main().catch(e=>{console.error(e);process.exit(1);});
module.exports={analysis,ingest,hash,verify};
