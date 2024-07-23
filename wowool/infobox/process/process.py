from wowool.native.core.pipeline import Pipeline
from multiprocessing import Process, Queue
from json import dumps as json_dumps
from json import loads as json_loads
from wowool.analysis import Analysis


def run_document(pipeline_desc: str, text: str):
    pipeline = Pipeline(pipeline_desc)
    return pipeline(text)


# must be a global function
def mt_process(q, pipeline_desc: str, text: str):
    doc = run_document(pipeline_desc, text)
    q.put(json_dumps(doc.to_json()))


def process(pipeline_desc: str, text: str):
    queue = Queue()
    p = Process(target=mt_process, args=(queue, pipeline_desc, text))
    p.start()
    p.join()  # this blocks until the process terminates
    json_str = queue.get()
    jo = json_loads(json_str)
    doc = Analysis.parse(jo["apps"]["wowool_analysis"]["results"])

    return doc
