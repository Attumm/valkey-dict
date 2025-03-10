"""Valkey Dict module."""
from typing import Any, Dict, Iterator, List, Tuple, Union, Optional, Type

from datetime import timedelta
from contextlib import contextmanager
from collections.abc import Mapping

from redis import StrictRedis

from .type_management import SENTINEL, EncodeFuncType, DecodeFuncType, EncodeType, DecodeType
from .type_management import _create_default_encode, _create_default_decode, _default_decoder
from .type_management import encoding_registry as enc_reg
from .type_management import decoding_registry as dec_reg


# pylint: disable=R0902, R0904
class ValkeyDict:
    """Python dictionary with Valkey as backend.

    With support for advanced features, such as custom data types, pipelining, and key expiration.

    This class provides a dictionary-like interface that interacts with a Valkey database, allowing
    for efficient storage and retrieval of key-value pairs. It supports various data types, including
    strings, integers, floats, lists, dictionaries, tuples, sets, and user-defined types. The class
    leverages the power of Valkey pipelining to minimize network round-trip time, latency, and I/O load,
    thereby optimizing performance for batch operations. Additionally, it allows for the management of
    key expiration through the use of context managers.

    The ValkeyDict class is designed to be analogous to a standard Python dictionary while providing
    enhanced functionality, such as support for a wider range of data types and efficient batch operations.
    It aims to offer a seamless and familiar interface for developers familiar with Python dictionaries,
    enabling a smooth transition to a Valkey-backed data store.

    Extendable Types: You can extend ValkeyDict by adding or overriding encoding and decoding functions.
    This functionality enables various use cases, such as managing encrypted data in Valkey,
    To implement this, simply create and register your custom encoding and decoding functions.
    By delegating serialization to valkey-dict, reduce complexity and have simple code in the codebase.
    """

    encoding_registry: EncodeType = enc_reg
    decoding_registry: DecodeType = dec_reg

    # pylint: disable=R0913
    def __init__(self,
             namespace: str = 'main',
             expire: Union[int, timedelta, None] = None,
             preserve_expiration: Optional[bool] = False,
             valkey: "Optional[StrictRedis[Any]]" = None,
             raise_key_error_delete: bool = False,
             **valkey_kwargs: Any) -> None:  # noqa: D202:R0913 pydocstyle clashes with Sphinx
        """
        Initialize a ValkeyDict instance.

        Init the ValkeyDict instance.

        Args:
            namespace (str): A prefix for keys stored in Valkey.
            expire (Union[int, timedelta, None], optional): Expiration time for keys.
            preserve_expiration (Optional[bool], optional): Preserve expiration on key updates.
            valkey (Optional[StrictValkey[Any]], optional): A Valkey connection instance.
            raise_key_error_delete (bool): Enable strict Python dict behavior raise if key not found when deleting.
            **valkey_kwargs (Any): Additional kwargs for Valkey connection if not provided.
        """

        self.namespace: str = namespace
        self.expire: Union[int, timedelta, None] = expire
        self.preserve_expiration: Optional[bool] = preserve_expiration
        self.raise_key_error_delete: bool = raise_key_error_delete
        if valkey:
            valkey.connection_pool.connection_kwargs["decode_responses"] = True

        self.valkey: StrictRedis[Any] = valkey or StrictRedis(decode_responses=True, **valkey_kwargs)
        self.get_valkey: StrictRedis[Any] = self.valkey

        self.custom_encode_method = "encode"
        self.custom_decode_method = "decode"

        self._iter: Iterator[str] = iter([])
        self._max_string_size: int = 500 * 1024 * 1024  # 500mb
        self._temp_valkey: Optional[StrictRedis[Any]] = None
        self._insertion_order_key = f"valkey-dict-insertion-order-{namespace}"
        self._batch_size: int = 200

    def _format_key(self, key: str) -> str:
        """
        Format a key with the namespace prefix.

        Args:
            key (str): The key to be formatted.

        Returns:
            str: The formatted key with the namespace prefix.
        """
        return f'{self.namespace}:{key}'

    def _parse_key(self, key: str) -> str:
        """
        Parse a formatted key with the namespace prefix and type.

        Args:
            key (str): The key to be parsed to type.

        Returns:
            str: The parsed key
        """
        return key[len(self.namespace) + 1:]

    def _valid_input(self, value: Any) -> bool:
        """
        Check if the input value is valid based on the specified value type.

        This method ensures that the input value is within the acceptable constraints for the given
        value type. For example, when the value type is "str", the method checks that the string
        length does not exceed the maximum allowed size (500 MB).

        Args:
            value (Any): The input value to be validated.

        Returns:
            bool: True if the input value is valid, False otherwise.
        """
        store_type = type(value).__name__
        if store_type == "str":
            return len(value) < self._max_string_size
        return True

    def _format_value(self, value: Any) -> str:
        """Format a valid value with the type and encoded representation of the value.

        Args:
            value (Any): The value to be encoded and formatted.

        Returns:
            str: The formatted value with the type and encoded representation of the value.
        """
        store_type = type(value).__name__
        encoded_value = self.encoding_registry.get(store_type, lambda x: x)(value)  # type: ignore
        return f'{store_type}:{encoded_value}'

    def _store_set(self, formatted_key: str, formatted_value: str) -> None:
        if self.preserve_expiration and self.get_valkey.exists(formatted_key):
            self.valkey.set(formatted_key, formatted_value, keepttl=True)
        else:
            self.valkey.set(formatted_key, formatted_value, ex=self.expire)

    def _store(self, key: str, value: Any) -> None:
        """
        Store a value in Valkey with the given key.

        Args:
            key (str): The key to store the value.
            value (Any): The value to be stored.

        Raises:
            ValueError: If the value or key fail validation.

        Note: Validity checks could be refactored to allow for custom exceptions that inherit from ValueError,
        providing detailed information about why a specific validation failed.
        This would enable users to specify which validity checks should be executed, add custom validity functions,
        and choose whether to fail on validation errors, or drop the data and only issue a warning and continue.
        Example use case is caching, to cache data only when it's between min and max sizes.
        Allowing for simple dict set operation, but only cache data that makes sense.

        """
        if not self._valid_input(value) or not self._valid_input(key):
            raise ValueError("Invalid input value or key size exceeded the maximum limit.")

        formatted_key = self._format_key(key)
        formatted_value = self._format_value(value)

        self._store_set(formatted_key, formatted_value)

    def _load(self, key: str) -> Tuple[bool, Any]:
        """
        Load a value from Valkey with the given key.

        Args:
            key (str): The key to retrieve the value.

        Returns:
            tuple: A tuple containing a boolean indicating whether the value was found and the value itself.
        """
        result = self.get_valkey.get(self._format_key(key))
        if result is None:
            return False, None
        return True, self._transform(result)

    def _transform(self, result: str) -> Any:
        """
        Transform the result string from Valkey into the appropriate Python object.

        Args:
            result (str): The result string from Valkey.

        Returns:
            Any: The transformed Python object.
        """
        type_, value = result.split(':', 1)
        return self.decoding_registry.get(type_, _default_decoder)(value)

    def new_type_compliance(
            self,
            class_type: type,
            encode_method_name: Optional[str] = None,
            decode_method_name: Optional[str] = None,
    ) -> None:
        """Check if a class complies with the required encoding and decoding methods.

        Args:
            class_type (type): The class to check for compliance.
            encode_method_name (str, optional): Name of encoding method of the class for valkey-dict custom types.
            decode_method_name (str, optional): Name of decoding method of the class for valkey-dict custom types.

        Raises:
            NotImplementedError: If the class does not implement the required methods when the respective check is True.
        """
        if encode_method_name is not None:
            if not (hasattr(class_type, encode_method_name) and callable(
                    getattr(class_type, encode_method_name))):
                raise NotImplementedError(
                    f"Class {class_type.__name__} does not implement the required {encode_method_name} method.")

        if decode_method_name is not None:
            if not (hasattr(class_type, decode_method_name) and callable(
                    getattr(class_type, decode_method_name))):
                raise NotImplementedError(
                    f"Class {class_type.__name__} does not implement the required {decode_method_name} class method.")

    # pylint: disable=too-many-arguments
    def extends_type(
            self,
            class_type: type,
            encode: Optional[EncodeFuncType] = None,
            decode: Optional[DecodeFuncType] = None,
            encoding_method_name: Optional[str] = None,
            decoding_method_name: Optional[str] = None,
    ) -> None: # noqa: D202 pydocstyle clashes with Sphinx
        """
        Extend ValkeyDict to support a custom type in the encode/decode mapping.

        This method enables serialization of instances based on their type,
        allowing for custom types, specialized storage formats, and more.
        There are three ways to add custom types:
        1. Have a class with an `encode` instance method and a `decode` class method.
        2. Have a class and pass encoding and decoding functions, where
        `encode` converts the class instance to a string, and
        `decode` takes the string and recreates the class instance.
        3. Have a class that already has serialization methods, that satisfies the:
        EncodeFuncType = Callable[[Any], str]
        DecodeFuncType = Callable[[str], Any]

        `custom_encode_method`
        `custom_decode_method`

        If no encoding or decoding function is provided, default to use the `encode` and `decode` methods of the class.

        The `encode` method should be an instance method that converts the object to a string.
        The `decode` method should be a class method that takes a string and returns an instance of the class.

        The method names for encoding and decoding can be changed by modifying the
        - `custom_encode_method`
        - `custom_decode_method`
        attributes of the ValkeyDict instance

        Example:
            >>> class Person:
            ...     def __init__(self, name, age):
            ...         self.name = name
            ...         self.age = age
            ...
            ...     def encode(self) -> str:
            ...         return json.dumps(self.__dict__)
            ...
            ...     @classmethod
            ...     def decode(cls, encoded_str: str) -> 'Person':
            ...         return cls(**json.loads(encoded_str))
            ...
            >>> valkey_dict.extends_type(Person)

        Args:
            class_type (type): The class `__name__` will become the key for the encoding and decoding functions.
            encode (Optional[EncodeFuncType]): function that encodes an object into a storable string format.
            decode (Optional[DecodeFuncType]): function that decodes a string back into an object of `class_type`.
            encoding_method_name (str, optional): Name of encoding method of the class for valkey-dict custom types.
            decoding_method_name (str, optional): Name of decoding method of the class for valkey-dict custom types.

        Raises:
            NotImplementedError

        Note:
            You can check for compliance of a class separately using the `new_type_compliance` method:

            This method raises a NotImplementedError if either `encode` or `decode` is `None`
            and the class does not implement the corresponding method.
        """

        if encode is None or decode is None:
            encode_method_name = encoding_method_name or self.custom_encode_method
            if encode is None:
                self.new_type_compliance(class_type, encode_method_name=encode_method_name)
                encode = _create_default_encode(encode_method_name)

            if decode is None:
                decode_method_name = decoding_method_name or self.custom_decode_method
                self.new_type_compliance(class_type, decode_method_name=decode_method_name)
                decode = _create_default_decode(class_type, decode_method_name)

        type_name = class_type.__name__
        self.decoding_registry[type_name] = decode
        self.encoding_registry[type_name] = encode

    def __eq__(self, other: Any) -> bool:
        """
        Compare the current ValkeyDict with another object.

        Args:
            other (Any): The object to compare with.

        Returns:
            bool: True if equal, False otherwise
        """
        if len(self) != len(other):
            return False
        for key, value in self.items():
            if value != other.get(key, SENTINEL):
                return False
        return True

    def __ne__(self, other: Any) -> bool:
        """
        Compare the current ValkeyDict with another object.

        Args:
            other (Any): The object to compare with.

        Returns:
            bool: False if equal, True otherwise
        """
        return not self.__eq__(other)

    def __getitem__(self, item: str) -> Any:
        """
        Get the value associated with the given key, analogous to a dictionary.

        Args:
            item (str): The key to retrieve the value.

        Returns:
            Any: The value associated with the key.

        Raises:
            KeyError: If the key is not found.
        """
        found, value = self._load(item)
        if not found:
            raise KeyError(item)
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set the value associated with the given key, analogous to a dictionary.

        Args:
            key (str): The key to store the value.
            value (Any): The value to be stored.
        """
        self._store(key, value)

    def __delitem__(self, key: str) -> None:
        """
        Delete the value associated with the given key, analogous to a dictionary.

        For distributed systems, we intentionally don't raise KeyError when the key doesn't exist.
        This ensures identical code running across different systems won't randomly fail
        when another system already achieved the deletion goal (key not existing).

        Warning:
            Setting dict_compliant=True will raise KeyError when key doesn't exist.
            This is not recommended for distributed systems as it can cause KeyErrors
            that are hard to debug when multiple systems interact with the same keys.

        Args:
            key (str): The key to delete

        Raises:
            KeyError: Only if dict_compliant=True and key doesn't exist
        """
        formatted_key = self._format_key(key)
        result = self.valkey.delete(formatted_key)
        if self.raise_key_error_delete and not result:
            raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        """
        Check if the given key exists in the ValkeyDict, analogous to a dictionary.

        Args:
            key (str): The key to check for existence.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        return self._load(key)[0]

    def __len__(self) -> int:
        """
        Get the number of items in the ValkeyDict, analogous to a dictionary.

        Returns:
            int: The number of items in the ValkeyDict.
        """
        return sum(1 for _ in self._scan_keys(full_scan=True))

    def __iter__(self) -> Iterator[str]:
        """
        Return an iterator over the keys of the ValkeyDict, analogous to a dictionary.

        Returns:
            Iterator[str]: An iterator over the keys of the ValkeyDict.
        """
        self._iter = self.keys()
        return self

    def __repr__(self) -> str:
        """
        Create a string representation of the ValkeyDict.

        Returns:
            str: A string representation of the ValkeyDict.
        """
        return str(self)

    def __str__(self) -> str:
        """
        Create a string representation of the ValkeyDict.

        Returns:
            str: A string representation of the ValkeyDict.
        """
        return str(self.to_dict())

    def __or__(self, other: Dict[str, Any]) -> Dict[str, Any]:
        """Implement the | operator (dict union).

        Returns a new dictionary with items from both dictionaries.

        Args:
            other (Dict[str, Any]): The dictionary to merge with.

        Raises:
            TypeError: If other does not adhere to Mapping.

        Returns:
            Dict[str, Any]: A new dictionary containing items from both dictionaries.
        """
        if not isinstance(other, Mapping):
            raise TypeError(f"unsupported operand type(s) for |: '{type(other).__name__}' and 'ValkeyDict'")

        result = {}
        result.update(self.to_dict())
        result.update(other)
        return result

    def __ror__(self, other: Dict[str, Any]) -> Dict[str, Any]:
        """
        Implement the reverse | operator.

        Called when ValkeyDict is on the right side of |.

        Args:
            other (Dict[str, Any]): The dictionary to merge with.

        Raises:
            TypeError: If other does not adhere to Mapping.

        Returns:
            Dict[str, Any]: A new dictionary containing items from both dictionaries.
        """
        if not isinstance(other, Mapping):
            raise TypeError(f"unsupported operand type(s) for |: 'ValkeyDict' and '{type(other).__name__}'")

        result = {}
        result.update(other)
        result.update(self.to_dict())
        return result

    def __ior__(self, other: Dict[str, Any]) -> 'ValkeyDict':
        """
        Implement the |= operator (in-place union).

        Modifies the current dictionary by adding items from other.

        Args:
            other (Dict[str, Any]): The dictionary to merge with.

        Raises:
            TypeError: If other does not adhere to Mapping.

        Returns:
            ValkeyDict: The modified ValkeyDict instance.
        """
        if not isinstance(other, Mapping):
            raise TypeError(f"unsupported operand type(s) for |: '{type(other).__name__}' and 'ValkeyDict'")

        self.update(other)
        return self

    @classmethod
    def __class_getitem__(cls: Type['ValkeyDict'], _key: Any) -> Type['ValkeyDict']:
        """
        Enable type hinting support like ValkeyDict[str, Any].

        Args:
            _key (Any): The type parameter(s) used in the type hint.

        Returns:
            Type[ValkeyDict]: The class itself, enabling type hint usage.
        """
        return cls

    def __reversed__(self) -> Iterator[str]:
        """
        Implement reversed() built-in.

        Returns an iterator over dictionary keys in reverse insertion order.

        Warning:
            ValkeyDict Currently does not support 'insertion order' as property thus also not reversed.

        Returns:
            Iterator[str]: An iterator yielding the dictionary keys in reverse order.
        """
        return reversed(list(self.keys()))

    def __next__(self) -> str:
        """
        Get the next item in the iterator.

        Returns:
            str: The next item in the iterator.
        """
        return next(self._iter)

    def next(self) -> str:
        """
        Get the next item in the iterator (alias for __next__).

        Returns:
            str: The next item in the iterator.

        """
        return next(self)

    def _create_iter_query(self, search_term: str) -> str:
        """
        Create a Valkey query string for iterating over keys based on the given search term.

        This method constructs a query by prefixing the search term with the namespace
        followed by a wildcard to facilitate scanning for keys that start with the
        provided search term.

        Args:
            search_term (str): The term to search for in Valkey keys.

        Returns:
            str: A formatted query string that can be used to find keys in Valkey.

        Example:
            >>> d = ValkeyDict(namespace='foo')
            >>> query = self._create_iter_query('bar')
            >>> print(query)
            'foo:bar*'
        """
        return f'{self.namespace}:{search_term}*'

    def _scan_keys(self, search_term: str = '', full_scan: bool = False) -> Iterator[str]:
        """Scan for Valkey keys matching the given search term.

        Args:
            search_term (str): A search term to filter keys. Defaults to ''.
            full_scan (bool): During full scan uses batches of self._batch_size by default 200

        Returns:
            Iterator[str]: An iterator of matching Valkey keys.
        """
        search_query = self._create_iter_query(search_term)
        count = None if full_scan else self._batch_size
        return self.get_valkey.scan_iter(match=search_query, count=count)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """Return the value for the given key if it exists, otherwise return the default value.

        Analogous to a dictionary's get method.

        Args:
            key (str): The key to retrieve the value.
            default (Optional[Any], optional): The value to return if the key is not found.

        Returns:
            Any: The value associated with the key or the default value.
        """
        found, item = self._load(key)
        if not found:
            return default
        return item

    def keys(self) -> Iterator[str]:
        """Return an Iterator of keys in the ValkeyDict, analogous to a dictionary's keys method.

        Returns:
            Iterator[str]: A list of keys in the ValkeyDict.
        """
        to_rm = len(self.namespace) + 1
        return (str(item[to_rm:]) for item in self._scan_keys())

    def key(self, search_term: str = '') -> Optional[str]:
        """Return the first value for search_term if it exists, otherwise return None.

        Args:
            search_term (str): A search term to filter keys. Defaults to ''.

        Returns:
            str: The first key associated with the given search term.
        """
        to_rm = len(self.namespace) + 1
        search_query = self._create_iter_query(search_term)
        _, data = self.get_valkey.scan(match=search_query, count=1)
        for item in data:
            return str(item[to_rm:])

        return None

    def items(self) -> Iterator[Tuple[str, Any]]:
        """Return a list of key-value pairs (tuples) in the ValkeyDict, analogous to a dictionary's items method.

        Yields:
            Iterator[Tuple[str, Any]]: A list of key-value pairs in the ValkeyDict.
        """
        to_rm = len(self.namespace) + 1
        for item in self._scan_keys():
            try:
                yield str(item[to_rm:]), self[item[to_rm:]]
            except KeyError:
                pass

    def values(self) -> Iterator[Any]:
        """Analogous to a dictionary's values method.

        Return a list of values in the ValkeyDict,

        Yields:
            List[Any]: A list of values in the ValkeyDict.
        """
        to_rm = len(self.namespace) + 1
        for item in self._scan_keys():
            try:
                yield self[item[to_rm:]]
            except KeyError:
                pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert the ValkeyDict to a Python dictionary.

        Returns:
            Dict[str, Any]: A dictionary with the same key-value pairs as the ValkeyDict.
        """
        return dict(self.items())

    def clear(self) -> None:
        """Remove all key-value pairs from the ValkeyDict in one batch operation using pipelining.

        This method mimics the behavior of the `clear` method from a standard Python dictionary.
        Valkey pipelining is employed to group multiple commands into a single request, minimizing
        network round-trip time, latency, and I/O load, thereby enhancing the overall performance.

        """
        with self.pipeline():
            for key in self._scan_keys(full_scan=True):
                self.valkey.delete(key)

    def _pop(self, formatted_key: str) -> Any:
        """
        Remove the value associated with the given key and return it.

        Or return the default value if the key is not found.

        Args:
            formatted_key (str): The formatted key to remove the value.

        Returns:
            Any: The value associated with the key or the default value.
        """
        return self.get_valkey.execute_command("GETDEL", formatted_key)

    def pop(self, key: str, default: Union[Any, object] = SENTINEL) -> Any:
        """Analogous to a dictionary's pop method.

        Remove the value associated with the given key and return it, or return the default value
        if the key is not found.

        Args:
            key (str): The key to remove the value.
            default (Optional[Any], optional): The value to return if the key is not found.

        Returns:
            Any: The value associated with the key or the default value.

        Raises:
            KeyError: If the key is not found and no default value is provided.
        """
        formatted_key = self._format_key(key)
        value = self._pop(formatted_key)
        if value is None:
            if default is not SENTINEL:
                return default
            raise KeyError(formatted_key)
        return self._transform(value)

    def popitem(self) -> Tuple[str, Any]:
        """Remove and return a random (key, value) pair from the ValkeyDict as a tuple.

        This method is analogous to the `popitem` method of a standard Python dictionary.

        if dict_compliant set true stays true to In Python 3.7+, removes the last inserted item (LIFO order)

        Returns:
            tuple: A tuple containing a randomly chosen (key, value) pair.

        Raises:
            KeyError: If ValkeyDict is empty.
        """
        while True:
            key = self.key()
            if key is None:
                raise KeyError("popitem(): dictionary is empty")
            try:
                return key, self.pop(key)
            except KeyError:
                continue

    def _create_set_get_command(self, formatted_key: str, formatted_value: str) -> Tuple[List[str], Dict[str, bool]]:
        """Create SET command arguments and options for Valkey. For setdefault operation.

        Args:
            formatted_key (str): The formatted Valkey key.
            formatted_value (str): The formatted value to be set.

        Returns:
            Tuple[List[str], Dict[str, bool]]: A tuple containing the command arguments and options.
        """
        # Setting {"get": True} enables parsing of the valkey result as "GET", instead of "SET" command
        options = {"get": True}
        args = ["SET", formatted_key, formatted_value, "NX", "GET"]
        if self.preserve_expiration:
            args.append("KEEPTTL")
        elif self.expire is not None:
            expire_val = int(self.expire.total_seconds()) if isinstance(self.expire, timedelta) else self.expire
            expire_str = str(1) if expire_val <= 1 else str(expire_val)
            args.extend(["EX", expire_str])
        return args, options

    def setdefault(self, key: str, default_value: Optional[Any] = None) -> Any:
        """Get value under key, and if not present set default value.

        Return the value associated with the given key if it exists, otherwise set the value to the
        default value and return it. Analogous to a dictionary's setdefault method.

        Args:
            key (str): The key to retrieve the value.
            default_value (Optional[Any], optional): The value to set if the key is not found.

        Returns:
            Any: The value associated with the key or the default value.
        """
        formatted_key = self._format_key(key)
        formatted_value = self._format_value(default_value)

        args, options = self._create_set_get_command(formatted_key, formatted_value)
        result = self.get_valkey.execute_command(*args, **options)

        if result is None:
            return default_value

        return self._transform(result)

    def copy(self) -> Dict[str, Any]:
        """Create a shallow copy of the ValkeyDict and return it as a standard Python dictionary.

        This method is analogous to the `copy` method of a standard Python dictionary

        Returns:
            dict: A shallow copy of the ValkeyDict as a standard Python dictionary.

        Note:
            does not create a new ValkeyDict instance.
        """
        return self.to_dict()

    def update(self, dic: Dict[str, Any]) -> None:
        """
        Update the ValkeyDict with key-value pairs from the given mapping, analogous to a dictionary's update method.

        Args:
            dic (Mapping[str, Any]): A mapping containing key-value pairs to update the ValkeyDict.
        """
        with self.pipeline():
            for key, value in dic.items():
                self[key] = value

    def fromkeys(self, iterable: List[str], value: Optional[Any] = None) -> 'ValkeyDict':
        """Create a new ValkeyDict from an iterable of key-value pairs.

        Create a new ValkeyDict with keys from the provided iterable and values set to the given value.
        This method is analogous to the `fromkeys` method of a standard Python dictionary, populating
        the ValkeyDict with the keys from the iterable and setting their corresponding values to the
        specified value.


        Args:
            iterable (List[str]): An iterable containing the keys to be added to the ValkeyDict.
            value (Optional[Any], optional): The value to be assigned to each key in the ValkeyDict. Defaults to None.

        Returns:
            ValkeyDict: The current ValkeyDict instance,populated with the keys from the iterable and their
            corresponding values.
        """
        for key in iterable:
            self[key] = value
        return self

    def __sizeof__(self) -> int:
        """Return the approximate size of the ValkeyDict in memory, in bytes.

        This method is analogous to the `__sizeof__` method of a standard Python dictionary, estimating
        the memory consumption of the ValkeyDict based on the serialized in-memory representation.
        Should be changed to valkey view of the size.

        Returns:
            int: The approximate size of the ValkeyDict in memory, in bytes.
        """
        return self.to_dict().__sizeof__()

    def chain_set(self, iterable: List[str], v: Any) -> None:
        """
        Set a value in the ValkeyDict using a chain of keys.

        Args:
            iterable (List[str]): A list of keys representing the chain.
            v (Any): The value to be set.
        """
        self[':'.join(iterable)] = v

    def chain_get(self, iterable: List[str]) -> Any:
        """
        Get a value from the ValkeyDict using a chain of keys.

        Args:
            iterable (List[str]): A list of keys representing the chain.

        Returns:
            Any: The value associated with the chain of keys.
        """
        return self[':'.join(iterable)]

    def chain_del(self, iterable: List[str]) -> None:
        """
        Delete a value from the ValkeyDict using a chain of keys.

        Args:
            iterable (List[str]): A list of keys representing the chain.
        """
        del self[':'.join(iterable)]

    #  def expire_at(self, sec_epoch: int | timedelta) -> Iterator[None]:
    #  compatibility with Python 3.9 typing
    @contextmanager
    def expire_at(self, sec_epoch: Union[int, timedelta]) -> Iterator[None]:
        """Context manager to set the expiration time for keys in the ValkeyDict.

        Args:
            sec_epoch (int, timedelta): The expiration duration is set using either an integer or a timedelta.

        Yields:
            ContextManager: A context manager during which the expiration time is the time set.
        """
        self.expire, temp = sec_epoch, self.expire
        yield
        self.expire = temp

    @contextmanager
    def pipeline(self) -> Iterator[None]:
        """
        Context manager to create a Valkey pipeline for batch operations.

        Yields:
            ContextManager: A context manager to create a Valkey pipeline batching all operations within the context.
        """
        top_level = False
        if self._temp_valkey is None:
            self.valkey, self._temp_valkey, top_level = self.valkey.pipeline(), self.valkey, True
        try:
            yield
        finally:
            if top_level:
                _, self._temp_valkey, self.valkey = self.valkey.execute(), None, self._temp_valkey  # type: ignore

    def multi_get(self, key: str) -> List[Any]:
        """
        Get multiple values from the ValkeyDict using a shared key prefix.

        Args:
            key (str): The shared key prefix.

        Returns:
            List[Any]: A list of values associated with the key prefix.
        """
        found_keys = list(self._scan_keys(key))
        if len(found_keys) == 0:
            return []
        return [self._transform(i) for i in self.valkey.mget(found_keys) if i is not None]

    def multi_chain_get(self, keys: List[str]) -> List[Any]:
        """
        Get multiple values from the ValkeyDict using a chain of keys.

        Args:
            keys (List[str]): A list of keys representing the chain.

        Returns:
            List[Any]: A list of values associated with the chain of keys.
        """
        return self.multi_get(':'.join(keys))

    def multi_dict(self, key: str) -> Dict[str, Any]:
        """
        Get a dictionary of key-value pairs from the ValkeyDict using a shared key prefix.

        Args:
            key (str): The shared key prefix.

        Returns:
            Dict[str, Any]: A dictionary of key-value pairs associated with the key prefix.
        """
        keys = list(self._scan_keys(key))
        if len(keys) == 0:
            return {}
        to_rm = keys[0].rfind(':') + 1
        return dict(
            zip([i[to_rm:] for i in keys], (self._transform(i) for i in self.valkey.mget(keys) if i is not None))
        )

    def multi_del(self, key: str) -> int:
        """
        Delete multiple values from the ValkeyDict using a shared key prefix.

        Args:
            key (str): The shared key prefix.

        Returns:
            int: The number of keys deleted.
        """
        keys = list(self._scan_keys(key))
        if len(keys) == 0:
            return 0
        return self.valkey.delete(*keys)

    def get_valkey_info(self) -> Dict[str, Any]:
        """
        Retrieve information and statistics about the Valkey server.

        Returns:
            dict: The information and statistics from the Valkey server in a dictionary.
        """
        return dict(self.valkey.info())

    def get_ttl(self, key: str) -> Optional[int]:
        """Get the Time To Live from Valkey.

        Get the Time To Live (TTL) in seconds for a given key. If the key does not exist or does not have an
        associated `expire`, return None.

        Args:
            key (str): The key for which to get the TTL.

        Returns:
            Optional[int]: The TTL in seconds if the key exists and has an expiry set; otherwise, None.
        """
        val = self.valkey.ttl(self._format_key(key))
        if val < 0:
            return None
        return val
