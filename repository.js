const {DatabaseSync}=require('node:sqlite');
const fs=require('node:fs');
class Repository {
 async init(){
  if(process.env.DB_MODE==='oracle'){
   const oracle=require('oracledb');
   this.connection=await oracle.getConnection({user:process.env.ORACLE_USER,password:process.env.ORACLE_PASSWORD,connectString:process.env.ORACLE_CONNECT_STRING});
   try{await this.connection.execute('CREATE TABLE PG2_STATE (ID NUMBER PRIMARY KEY, PAYLOAD CLOB NOT NULL)');}catch(e){if(e.errorNum!==955)throw e;}
   this.mode='Oracle';
  }else{
   fs.mkdirSync(process.env.DATA_DIR||'data',{recursive:true});
   this.db=new DatabaseSync(`${process.env.DATA_DIR||'data'}/energia.sqlite`);
   this.db.exec('PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS pg2_state(id INTEGER PRIMARY KEY,payload TEXT NOT NULL);');
   this.mode='SQLite local';
  }
 }
 async load(){
  if(this.connection){const o=require('oracledb');const r=await this.connection.execute('SELECT PAYLOAD FROM PG2_STATE WHERE ID=1',[],{fetchInfo:{PAYLOAD:{type:o.STRING}}});return r.rows.length?JSON.parse(r.rows[0][0]):null;}
  const r=this.db.prepare('SELECT payload FROM pg2_state WHERE id=1').get();return r?JSON.parse(r.payload):null;
 }
 async save(state){const payload=JSON.stringify(state);if(this.connection){const oracle=require('oracledb');this.pending=(this.pending||Promise.resolve()).then(()=>this.connection.execute('MERGE INTO PG2_STATE d USING (SELECT 1 ID FROM DUAL) s ON (d.ID=s.ID) WHEN MATCHED THEN UPDATE SET d.PAYLOAD=:payload WHEN NOT MATCHED THEN INSERT(ID,PAYLOAD) VALUES(1,:payload)',{payload:{val:payload,type:oracle.CLOB}},{autoCommit:true}));await this.pending;}else this.db.prepare('INSERT INTO pg2_state VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload').run(payload);}
 async close(){if(this.connection)await this.connection.close();else this.db.close();}
}
module.exports={Repository};
