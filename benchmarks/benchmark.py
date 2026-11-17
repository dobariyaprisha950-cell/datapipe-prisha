"""Reproducible normalizer benchmark; results are printed, never prefilled."""
import argparse,json,time,threading
import psutil
from datapipe.transforms.parallel import normalize,normalize_parallel
def main()->None:
 p=argparse.ArgumentParser();p.add_argument("--records",type=int,default=200_000);p.add_argument("--workers",type=int,default=4);a=p.parse_args()
 rows=[{"value":float(i%997)} for i in range(a.records)];results={}
 outputs={}
 process=psutil.Process();
 for name,fn in (("pure",lambda:normalize(rows,"value")),("parallel",lambda:normalize_parallel(rows,"value",a.workers))):
  peak=[0];stop=threading.Event()
  def sample()->None:
   while not stop.is_set():
    total=0
    for proc in [process,*process.children(recursive=True)]:
     try:total+=proc.memory_info().rss
     except psutil.Error:pass
    peak[0]=max(peak[0],total);stop.wait(.01)
  sampler=threading.Thread(target=sample,daemon=True);sampler.start();start=time.perf_counter();out=fn();elapsed=time.perf_counter()-start;stop.set();sampler.join()
  outputs[name]=out;results[name]={"seconds":elapsed,"records_per_second":len(out)/elapsed,"peak_rss_bytes_including_children":peak[0]}
 if outputs["pure"]!=outputs["parallel"]:raise RuntimeError("parallel output differed from sequential output")
 results["speedup"]=results["pure"]["seconds"]/results["parallel"]["seconds"];results["environment"]={"python":__import__("platform").python_version(),"workers":a.workers,"records":a.records};print(json.dumps(results,indent=2))
if __name__=="__main__":main()
