from bluesky.plans import count

from suitcase import nxsas


def test_run(RE, tmp_path):
    print(RE.md)

    document_list = list()

    def store_documents(name, doc):
        document_list.append((name, doc))

    RE.subscribe(store_documents)

    RE(count([]), md={"techniques": list()})

    assert len(document_list) > 0

    nxsas.export(gen=document_list, directory=tmp_path)
