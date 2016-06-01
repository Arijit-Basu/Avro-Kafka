import json
import os

from json_avro.create_avro_schema import CreateAvroSchema


def create_avro_schema_file(avro_json_schema, name):
    """
    Create a file (*.avsc) under schema directory.
    Created file contain avro schema.
    :param avro_json_schema: It is created avro schema from passed JSON
    :param name:  Name of avro schema
    :return:
    """
    path = os.getcwd()
    directory = path + "/schema"
    if not os.path.exists(directory):
        os.makedirs(directory)
        file_name = directory + "/{0}.avsc".format(name)
        with open(file_name, "w") as file:
            json.dump(avro_json_schema, file, sort_keys=True, indent=4, ensure_ascii=False)
    else:
        file_name = directory + "/{0}.avsc".format(name)
        with open(file_name, "w") as file:
            json.dump(avro_json_schema, file, sort_keys=True, indent=4, ensure_ascii=False)


def main_function(file_name):
    """
    Read the JSON from file and get the name from user input.
    Pass all the parameter to the create_avro_schema.
    :param file_name: File which contain JSON
    :return:
    """
    with open(file_name, 'r') as file:
        json_value = json.loads(file.read())
        print json_value, type(json_value)
        name = raw_input("Enter name of the avro for a JSON")
    avro_json_schema = CreateAvroSchema.create_avro_schema(json_value, name)
    create_avro_schema_file(avro_json_schema, name)


if __name__ == '__main__':
    print "You are in the main Function"
    file_name = os.getcwd() + "/json_value.json"
    main_function(file_name)
