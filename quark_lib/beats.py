from __future__ import print_function
import json
## This breaks my quark wont require external packages vow, but json schema isn't so bad. :)
try:
    from jsonschema import validate
except:
    # This should be WARN or something like that
    print("jsonschema package isn't available, will just assume you know what you're doing")

class Beat(object):

    # TODO: Validation
    def __init__(self, schema_str):
        try:
            self.schema = json.loads(schema_str)
        except:
            raise Exception("Invalid json schema supplied: {}".format(schema_str))

    def load(self, beat_obj):
        assert type(beat_obj) == dict
        if self.is_valid(beat_obj):
            self.data = beat_obj
            return self.data




    def is_valid(self, json_obj):
        assert type(json_obj) == dict

        # TODO: catch exception, be lazy with finding all the issues
        basic = validate(json_obj, self.schema)

        if basic != None:
            return False


        num, unit = json_obj['frequency'].split(" ") # this will break if you have more than one space. :/
        if unit.lower() not in ["minute", "minutes", "hour", "hours", "day", "days", "week", "weeks"]:
            raise Exception("unknown unit " + unit)
        freqint = int(num) # also will throw exception if not int or if not a number
        owners = json_obj['owner'].split(",")
        if len(owners) == 0:
            raise Exception("Need one or more owners for the batch job.")
        for owner in owners:
            # the lamest email validator you'll ever see
            if not '@' in owner:
                raise Exception("{} doesn't look like a valid email.".format(owner))
        return True


    def __str__(self):
        if self.data == None:
            raise Exception("No beat config has been loaded. Run load(<dict>)")
        return json.dumps(self.data, indent=4, sort_keys=True)
