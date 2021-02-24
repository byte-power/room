import itertools
import os
import random
import sys
import uuid

chinese_chars = "丞相祠堂何处寻锦官城外柏森森映阶碧草自春色隔叶黄鹂空好音三顾频烦天下计两朝开济老臣心出师未捷身先死长使英雄泪满襟"


def generate_redis_protocol(*args):
    command = "*{len}\r\n".format(len=len(args))
    for arg in args:
        command = command + "${len}\r\n{arg}\r\n".format(len=len(arg.encode("utf-8")), arg=arg)
    return command


def generate_key(t):
    modules = ["account", "asset", "storage", "pigat"]
    app_id = uuid.uuid4().hex[:7]
    user_id = uuid.uuid4().hex[:13]
    random_key = uuid.uuid4().hex[:5] + "".join(random.choices(chinese_chars, k=5))
    module = random.choice(modules)
    return "{app_id}:{module}:{user_id}:{random_key}:{t}".format(
        app_id=app_id, module=module,
        user_id="{"+user_id+"}", random_key=random_key, t=t)


def generate_value(length):
    if length % 2 == 0:
        ascii_char_count = length // 2
        chinese_char_count = length // 2
    else:
        ascii_char_count = length // 2 + 1
        chinese_char_count = length // 2
    ascii_str = "".join(random.choices(uuid.uuid4().hex, k=ascii_char_count))
    chinese_str = "".join(random.choices(chinese_chars, k=chinese_char_count))
    return ascii_str + chinese_str


def generate_float():
    return str(random.random() * 100)


def generate_string_commands(count):
    for _ in range(count):
        key = generate_key("str")
        value = generate_value(32)
        yield generate_redis_protocol("set", key, value)


def generate_set_commands(count):
    for _ in range(count):
        key = generate_key("set")
        values = []
        for _ in range(102):
            value = generate_value(10)
            values.append(value)
        yield generate_redis_protocol("sadd", key, *values)


def generate_list_commands(count):
    for _ in range(count):
        key = generate_key("list")
        values = []
        for _ in range(102):
            value = generate_value(10)
            values.append(value)
        yield generate_redis_protocol("rpush", key, *values)


def generate_hash_commands(count):
    for _ in range(count):
        key = generate_key("hash")
        values = []
        for _ in range(102):
            field = generate_value(10)
            value = generate_value(10)
            values.append(field)
            values.append(value)
        yield generate_redis_protocol("hset", key, *values)


def generate_zset_commands(count):
    for _ in range(count):
        key = generate_key("zset")
        values = []
        for _ in range(102):
            score = generate_float()
            member = generate_value(10)
            values.append(score)
            values.append(member)
        yield generate_redis_protocol("zadd", key, *values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python generate_raw_redis_command.py count")
        exit()

    count = int(sys.argv[1])
    commands = []
    print(
        "generate {} commands for string, list, set, hash and zset".format(count),
        file=sys.stderr,
    )

    commands = itertools.chain(
        generate_string_commands(count),
        generate_list_commands(count),
        generate_set_commands(count),
        generate_hash_commands(count),
        generate_zset_commands(count)
    )
    index = 0
    for command in commands:
        print(command, end="")
        if index % 10000 == 0:
            sys.stdout.flush()
        index += 1
