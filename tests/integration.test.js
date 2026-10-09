const {test}=require('node:test');const assert=require('node:assert/strict');const {spawn}=require('node:child_process');const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const testAdminPassword=require('node:crypto').randomBytes(24).toString('base64url');
const testUserPassword=require('node:crypto').randomBytes(24).toString('base64url');
test('Flujo completo: permisos, MQTT, energía, anomalías, pérdida, informes y persistencia',async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'pg2-test-'));let child,output='';const base='http://127.0.0.1:3107';
 async function start(){child=spawn(process.execPath,['server.js'],{cwd:path.join(__dirname,'..'),env:{...process.env,PORT:'3107',MQTT_PORT:'18887',DATA_DIR:dir,ADMIN_PASSWORD:testAdminPassword}});child.stdout.on('data',c=>output+=c);child.stderr.on('data',c=>output+=c);for(let i=0;i<100;i++){try{await fetch(base);return;}catch{await sleep(100);}}throw new Error(output);}
 async function stop(){const done=new Promise(r=>child.once('exit',r));child.kill('SIGTERM');await done;}
 async function api(route,b,cookie=''){const r=await fetch(base+route,{method:b===undefined?'GET':'POST',headers:{...(b===undefined?{}:{'Content-Type':'application/json'}),Cookie:cookie},body:b===undefined?undefined:JSON.stringify(b)});const isJson=r.headers.get('content-type')?.includes('json');const data=isJson?await r.json():Buffer.from(await r.arrayBuffer());return {status:r.status,data,cookie:r.headers.get('set-cookie')?.split(';')[0]};}
 try{
 await start();assert.equal((await api('/api/state')).status,401);
 const login=await api('/api/login',{username:'admin',password:testAdminPassword});assert.equal(login.status,200);const admin=login.cookie;
 const post=async(r,b,c=admin)=>{const x=await api(r,b,c);assert.equal(x.status,200,JSON.stringify(x.data));return x.data;};
 let s=(await api('/api/state',undefined,admin)).data;assert.equal(s.health.database,'SQLite local');assert.equal(s.health.mqtt,true);assert.equal(s.analysis.indicators.length,17);assert.equal(s.analysis.indicators[0].value,null);assert.equal(s.analysis.indicators[15].value,null);
 for(const role of ['Técnico','Consulta'])await post('/api/users',{username:role==='Técnico'?'tecnico':'consulta',name:role,role,active:true,password:testUserPassword});
 const tech=(await api('/api/login',{username:'tecnico',password:testUserPassword})).cookie;const read=(await api('/api/login',{username:'consulta',password:testUserPassword})).cookie;
 assert.equal((await api('/api/simulation',{running:true},read)).status,403);assert.equal((await api('/api/rules',{pointId:'PM-001'},tech)).status,403);assert.equal((await api('/api/reports',{title:'No permitido'},read)).status,403);
 assert.deepEqual((await api('/api/state',undefined,read)).data.users,[]);
 const id=s.users[0].id;assert.equal((await api('/api/users',{id,username:'admin',name:'Admin',role:'Consulta',active:true},admin)).status,400);
 assert.equal((await api('/api/points',{id:'BAD',name:'BAD',areaId:'missing',baseCurrent:8,voltage:120,pf:1,interval:0,active:true},admin)).status,400);
 await post('/api/points',{...s.points[0],interval:1,baseCurrent:8,voltage:120,pf:1,edit:true});
 await post('/api/simulation',{pointId:'PM-001',scenario:'sobrecarga',loss:0,delay:0});await post('/api/simulation',{running:true});
 for(let i=0;i<50;i++){await sleep(150);s=(await api('/api/state',undefined,admin)).data;if(s.events.length&&s.measurements.some(m=>m.pointId==='PM-001'&&m.energyDelta>0))break;}
 const m=s.measurements.find(m=>m.pointId==='PM-001');assert.ok(m);assert.equal(m.current,18.4);assert.ok(Math.abs(m.power-2.208)<1e-9);assert.ok(m.energyTotal>0);assert.ok(s.events.length);assert.equal(s.events[0].status,'Abierta');assert.ok(s.analysis.indicators[9].value>0);
 assert.equal((await api('/api/events',{id:s.events[0].id,status:'Cerrada',note:'Cierre prematuro'},admin)).status,400);
 await post('/api/simulation',{pointId:'PM-001',scenario:'normal',loss:0,delay:0});await sleep(1400);s=(await api('/api/state',undefined,admin)).data;assert.ok(s.events[0].end);await post('/api/events',{id:s.events[0].id,status:'Cerrada',note:'Recuperación verificada'});
 await post('/api/reports',{title:'Prueba funcional MQTT'},tech);s=(await api('/api/state',undefined,admin)).data;assert.equal(s.reports.length,1);const rid=s.reports[0].id;
 const pdf=await api(`/api/reports/${rid}/pdf`,undefined,read);assert.equal(pdf.status,200);assert.equal(pdf.data.subarray(0,4).toString(),'%PDF');assert.match((await api(`/api/reports/${rid}/csv`,undefined,read)).data.toString(),/SIMULADO/);
 const before=s.points[0].received;await post('/api/simulation',{pointId:'PM-001',scenario:'normal',loss:100,delay:0});await sleep(3400);s=(await api('/api/state',undefined,admin)).data;assert.equal(s.points[0].received,before);assert.equal(s.points[0].communication,'Sin comunicación');assert.ok(s.transport.some(t=>t.status==='Perdido'));assert.ok(s.analysis.coverage<100);
 await post('/api/simulation',{pointId:'PM-001',scenario:'normal',loss:0,delay:1300});for(let i=0;i<20;i++){await sleep(100);s=(await api('/api/state',undefined,admin)).data;if(s.health.queued>0)break;}assert.ok(s.health.queued>0);await sleep(1800);s=(await api('/api/state',undefined,admin)).data;assert.ok(s.points[0].received>before);
 assert.equal((await api('/api/state?from=invalid',undefined,admin)).status,400);
 const count=s.health.received;await stop();await start();const relog=(await api('/api/login',{username:'admin',password:testAdminPassword})).cookie;s=(await api('/api/state',undefined,relog)).data;assert.ok(s.health.received>=count);assert.equal(s.events[0].status,'Cerrada');assert.equal(s.reports.length,1);assert.equal(s.health.running,false);assert.equal((await api('/api/state',undefined,admin)).status,401);
 await post('/api/logout',{},relog);assert.equal((await api('/api/state',undefined,relog)).status,401);
 }finally{if(child&&child.exitCode===null)await stop();fs.rmSync(dir,{recursive:true,force:true});}
});
