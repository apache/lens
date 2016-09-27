# lens-python-client
Lens Python Client

## Installation
You can install like this:

    pip install -e 'git+https://github.com/apache/lens.git#egg=lenspythonclient&subdirectory=contrib/clients/python' # install a python package from a repo subdirectory

## Local development

    For local development, fork the project, build with profile `py` (`mvn clean install -Ppy`). That should link python client to 
    your PYTHONPATH. After that, all the changes you make in client here are accessible to your other applications that depend 
    the python client. If that fails, going inside python client directory (contrig/clients/python) and running `python setup.py develop` or 
    `python setup.py develop --user` should work.

## Usage

### Listing queries
```python
with LensClient("http://lens.server.url/", "user.name", database="db") as client:
    for handle in client.queries(state='RUNNING'):
        print client.queries[handle]
```

### Async submission
``` python
from lens.client import LensClient
with LensClient("http://lens.server.url/", "user.name", database="db") as client:
    # Submit asynchronously
    handle = client.queries.submit("cube select ...", query_name="My first query")
    # You can wait for completion and get the entire query detail:
    query = client.queries.wait_till_finish(handle, poll_interval=5) # poll each 5 seconds
    # the path would be accessible from lens server machine. Not much useful for the client
    print query.result_set_path
    # iterating over result:
    for row in query.result:
        print row
    # Writing result to local csv:
    with open('result.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        for row in query.result: # This will fetch the result again from lens server
            writer.writerow(row)
    # listing queries:
    for handle in client.queries(state='RUNNING'):
        print client.queries[handle]
```

### Sync submission
```python
from lens.client import LensClient
with LensClient("http://lens.server.url/", "user.name", database="db") as client:
    # Half async: The http call will return in 10 seconds, post that, query would be cancelled (depending on the server's configurations)
    query = client.queries.submit("cube select ...", query_name="My first query", timeout=10) # 10 seconds
    if query.status.status != 'CANCELLED':
        result = query.result
```

### Async submission with wait
```python
from lens.client import LensClient
with LensClient("http://lens.server.url/", "user.name", database="db") as client:
    # Pseudo-sync
    query = client.queries.submit("cube select ...", query_name="My first query", wait=True, poll_interval=5) # submit async and wait till finish, polling every 5 seconds. poll_interval is optional
    query.result
```

### Fetching just results
```python
from lens.client import LensClient
with LensClient("http://lens.server.url/", "user.name", database="db") as client:
    # Direct result. Query handle and other details will be lost. 
    result = client.queries.submit("cube select ...", query_name="My first query", fetch_result=True, poll_interval=5, delimiter=",", custom_mappings={})
```
