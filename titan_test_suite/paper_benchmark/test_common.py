import json
from .common import bootstrap_median_ci,latency,load_graph,summarize
def test_statistics_are_deterministic():
    assert summarize([1,2,3,4,5])["median"]==3
    assert bootstrap_median_ci([1,2,3,4,5])==bootstrap_median_ci([1,2,3,4,5])
    assert latency([1,2,3,4,5])["p99"]>4.9
def test_wfcommons_16_layout(tmp_path):
    p=tmp_path/"wf.json"; p.write_text(json.dumps({"workflow":{"specification":{"tasks":[
      {"id":"a","name":"root","parents":[],"children":["b"]},{"id":"b","name":"leaf","parents":["a"],"children":[]}]},
      "execution":{"tasks":[{"id":"a","command":{"program":"wfbench"}},{"id":"b","command":{"program":"wfbench"}}]}}}))
    _,tasks,parents=load_graph(p)
    assert list(tasks)==["a","b"] and parents=={"a":[],"b":["a"]}
    assert tasks["a"]["command"]["program"]=="wfbench"
