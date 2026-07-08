class QueryResult:
    def __init__(self, roworder, query, targettbl, appname=None, _id=None, status=None, namespace=None,
                 started_datetime=None, finished_datetime=None, notes=None, uniquerunid=None,
                 iscurrentrow=True):
        self.roworder = roworder
        self.id = _id
        self.query = query
        self.appname = appname
        self.targettbl = targettbl
        self.status = status
        self.namespace = namespace
        self.started_datetime = started_datetime
        self.finished_datetime = finished_datetime
        self.notes = notes
        self.uniquerunid = uniquerunid
        self.iscurrentrow = iscurrentrow
