class CreateAvroSchema(object):
    """
    Class for creating Avro schema from JSON
    """

    def __init__(self):
        pass

    @classmethod
    def create_avro_schema(cls, json_value, schema_name):
        """

        :param json_value: It is a JSON value
        :param schema_name: Name of Avro schema
        :return: Avro schema
        """
        key_list = cls.parse_json_key(json_value)
        avro_json = {
            "namespace": "{0}avro.schema".format(schema_name),
            "type": "record",
            "name": schema_name,

            "fields": [{
                "name": "data_version",
                "type": "string"
            },
                {
                    "name": "ip_address",
                    "type": "string"
                },

            ]
        }
        avro_json = cls.add_fields(key_list, json_value, avro_json)
        return avro_json

    @classmethod
    def parse_json_key(cls, json_value):
        """
        It take JSON value and return key,type of value stored in key and value of a key
        :param json_value: JSON value
        :return: List of tuples (key,type(value),value)
        """
        key_list = [{
                        "key": key,
                        "type": type(value),
                        "value": value
                    } for key, value in json_value.iteritems()
                    ]
        return key_list

    @classmethod
    def get_array_schema(cls, json_value, key):
        """
        It return specific Avro schema of a key that contain a value as a list type
        :param json_value: It is a JSON value of a list
        :param key: name of a key that contains a value as list
        :return: return Avro schema of a key that contain list value
        """
        value = json_value[key]
        json_value = value[0]
        key_list = cls.parse_json_key(json_value)
        avro_json = {
            "type": "record",
            "name": "{0}_records".format(key),
            "fields": [

            ]
        }
        avro_json = cls.add_fields(key_list, json_value, avro_json)
        return avro_json

    @classmethod
    def add_fields(cls, key_list, json_value, avro_json):
        """
        It take created avro schema and append all fields in it by iterating over key_list
        :param key_list: It is key_list return by a function parse_json_key
        :param json_value: JSON
        :param avro_json: Avro_json for appending fields
        :return: Updated Avro schema (Appending the fields depending on key in key_list)
        """
        for dict in key_list:
            dummy_list = []
            dummy_map = {}
            if dict["type"] == type(dummy_list):
                value_key = dict["value"][0]
                if type(value_key) == type(dummy_map):
                    val = cls.get_array_schema(json_value, dict["key"])
                    avro_json['fields'].append({
                        "name": dict["key"],
                        "type": {
                            "type": "array",
                            "items": val
                        }
                    })
                else:
                    avro_json['fields'].append({
                        "name": dict["key"],
                        "type": {
                            "type": "array",
                            "items": cls.get_datatype(type(json_value[dict["key"]][0]))
                        }
                    })
            elif dict["type"] == type(dummy_map):
                key = dict["key"]
                json_val = dict["value"]
                data = cls.get_dict_schema(json_val, key)
                avro_json["fields"].append(data)
            else:
                datatype = cls.get_datatype(dict["type"])
                avro_json['fields'].append({
                    "name": dict["key"],
                    "type": datatype
                })

        return avro_json

    @classmethod
    def get_datatype(cls, datatype):
        """
        Convert Python datatype to Avro datatype
        :param datatype: Datatype of python
        :return: Datatype of Avro
        """
        data = 0.0000000
        if datatype == type(str(data)):
            return "string"
        if datatype == type(long(data)):
            return "long"
        if datatype == type(int(data)):
            return "int"
        if datatype == type(float(data)):
            return "float"
        if datatype == type(bool(data)):
            return "boolean"

    @classmethod
    def get_dict_schema(cls, json_value, key):
        """
        It return specific Avro schema of a key that contain a value as a dictionary type
        :param json_value: It is a JSON value of a dictionary
        :param key: name of a key that contains a value as dictionary
        :return: return Avro schema of a key that contain dictionary value
        """
        key_list = cls.parse_json_key(json_value)
        main_avro_json = {
            "name": key,
            "type": {

            }
        }
        sub_avro_json = {"name": "{0}_records".format(key),
                         "type": "record",
                         "fields": [

                         ]
                         }
        sub_avro_json = cls.add_fields(key_list, json_value, sub_avro_json)
        main_avro_json["type"] = sub_avro_json
        return main_avro_json
