import argparse
import textwrap

SQL = {
    "data": {
        "create": textwrap.dedent('''
            CREATE TABLE public.room_data_v2_{db_index} (
                hash_tag character varying NOT NULL,
                value jsonb NOT NULL,
                deleted_at timestamp with time zone DEFAULT NULL,
                updated_at timestamp with time zone NOT NULL DEFAULT now(),
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                version bigint NOT NULL DEFAULT 0
            );

            ALTER TABLE ONLY public.room_data_v2_{db_index}
                ADD CONSTRAINT room_data_v2_{db_index}_pkey PRIMARY KEY (hash_tag);
            '''),

        "count": "select 'room_data_v2_{db_index}' as table_name, count(*) as count from room_data_v2_{db_index}",
        "truncate": "truncate table room_data_v2_{db_index};",
        "sum": "select sum(count), 'room_data_v2' as table_name from ({sql}) as t;",
    },
    "keys": {
        "create": textwrap.dedent('''
            CREATE TABLE public.room_hash_tag_keys_{db_index} (
                hash_tag character varying NOT NULL,
                keys text[] NOT NULL,
                accessed_at timestamp with time zone NOT NULL,
                written_at timestamp with time zone DEFAULT NULL,
                synced_at timestamp with time zone DEFAULT NULL,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                updated_at timestamp with time zone NOT NULL DEFAULT now(),
                status character varying NOT NULL,
                version bigint NOT NULL DEFAULT 0
            );

            ALTER TABLE ONLY public.room_hash_tag_keys_{db_index}
                ADD CONSTRAINT room_hash_tag_keys_{db_index}_pkey PRIMARY KEY (hash_tag);

            CREATE INDEX room_hash_tag_keys_status_accessed_at_{db_index}_idx ON public.room_hash_tag_keys_{db_index} USING btree (status, accessed_at);

            CREATE INDEX room_hash_tag_keys_status_written_at_{db_index}_idx ON public.room_hash_tag_keys_{db_index} USING btree (status, written_at);
        '''),
        "count": "select 'room_hash_tag_keys_{db_index}' as table_name, count(*) as count from room_hash_tag_keys_{db_index}",
        "truncate": "truncate table room_hash_tag_keys_{db_index};",
        "sum": "select sum(count), 'room_hash_tag_keys' as table_name from ({sql}) as t;",
    },
}


def generate_sql(database, sql_type, table, start_index, end_index):
    joins = {
        "create": "\n",
        "truncate": "\n",
        "count": "\nunion all\n",
        "sum": "\nunion all\n",
    }
    connct_db_sql = "\c {database}".format(database=database)
    sqls = []
    if sql_type == "sum":
        sql_template = SQL[table]["count"]
    else:
        sql_template = SQL[table][sql_type]
    for i in range(start_index, end_index+1):
        sql = sql_template.format(db_index=i)
        sqls.append(sql)
    join_str = joins[sql_type]
    final_sql = join_str.join(sqls)
    if sql_type == "sum":
        final_sql = SQL[table][sql_type].format(sql=final_sql)
    final_sql = "\n".join([connct_db_sql, final_sql])
    if sql_type == "count":
        final_sql = final_sql + ";"
    return final_sql


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL")
    parser.add_argument(
        "--sql",
        choices=["create", "count", "sum", "truncate"],
        required=True)
    parser.add_argument("-d", "--database", required=True)
    parser.add_argument(
        "-t", "--table",
        choices=["data", "keys"],
        required=True)
    parser.add_argument("-s", "--start_index", type=int, required=True)
    parser.add_argument("-e", "--end_index", type=int, required=True)
    args = parser.parse_args()
    sql = generate_sql(
        args.database, args.sql, args.table,
        args.start_index, args.end_index)
    print(sql)
