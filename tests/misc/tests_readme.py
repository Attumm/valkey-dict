from valkey_dict import ValkeyDict
### Insertion Order
from valkey_dict import PythonValkeyDict

dic = PythonValkeyDict()
dic["1"] = "one"
dic["2"] = "two"
dic["3"] = "three"

assert list(dic.keys()) == ["1", "2", "3"]

### Extending ValkeyDict with Custom Types
import json

class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def encode(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def decode(cls, encoded_str: str) -> 'Person':
        return cls(**json.loads(encoded_str))

valkey_dict = ValkeyDict()

# Extend valkey dict with the new type
valkey_dict.extends_type(Person)

# ValkeyDict can now seamlessly handle Person instances.
person = Person(name="John", age=32)
valkey_dict["person1"] = person

result = valkey_dict["person1"]

assert result.name == person.name
assert result.age == person.age