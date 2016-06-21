# lens-python-client
Lens Python Client

## Installation
You can install like this:

    pip install -e 'git+https://github.com/apache/lens.git#egg=lenspythonclient&subdirectory=contrib/clients/python' # install a python package from a repo subdirectory


## Usage
    from lens.client import LensClient
    with LensClient("http://lens.server.url/", "user.name", database="db") as client:
        handle = client.queries.submit("cube select ...", query_name="My first query")
        # Optionally wait for completion.
        while not client.queries[handle].finished:
            time.sleep(20) # sleep 20 seconds
        print client.queries[handle].result_set_path
        # listing queries:
        for handle in client.queries(state='RUNNING'):
            print client.queries[handle]
