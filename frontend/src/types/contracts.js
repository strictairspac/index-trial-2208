/** @typedef {{id:number,name:string,sensor_id:number,start_at:string,end_at:string,device_type:string|null}} Workload */
/** @typedef {{id:number,name:string,workload_id:number,columns:string[]}} Candidate */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null}} Experiment */
/** @typedef {{seq:number|null,op:'insert'|'update'|'snapshot',telemetry_id:number,data:object}} ChangeEvent */
/** @typedef {{id:number,subscription_id:number,kind:'history'|'changes',from_cursor:number,to_cursor:number,last_backfill:boolean,events:ChangeEvent[],event_count:number,attempts:number,acked_at:string|null}} ChangeBatch */
/** @typedef {{id:number,name:string,from_id:number,batch_size:number,phase:'history'|'changes',backfill_high:number,seq_start:number,backfill_cursor:number,seq_cursor:number}} Subscription */
export {};
