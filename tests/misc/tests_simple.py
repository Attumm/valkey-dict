from datetime import datetime

from valkey_dict import ValkeyDict, PythonValkeyDict

for obj in [ValkeyDict, PythonValkeyDict]:
    dic = obj(namespace='assert_test')
    assert 'random' not in dic
    dic['random'] = 4
    assert dic['random'] == 4
    assert 'random' in dic
    del dic['random']
    assert 'random' not in dic

    now = datetime.now()
    dic['datetime'] = now
    assert dic['datetime'] == now
    dic.clear()

print("passed assert test")
