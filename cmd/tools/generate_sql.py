import argparse
import textwrap

SQL = {
    "data": {
        "create": textwrap.dedent('''
            CREATE TABLE public.room_data_{db_index} (
                key character varying NOT NULL,
                type character varying NOT NULL,
                value character varying NOT NULL,
                deleted boolean NOT NULL DEFAULT false,
                updated_at timestamp with time zone NOT NULL DEFAULT now(),
                synced_at timestamp with time zone NOT NULL DEFAULT now(),
                expire_at timestamp with time zone,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                version bigint NOT NULL DEFAULT 0
            );

            ALTER TABLE ONLY public.room_data_{db_index}
                ADD CONSTRAINT room_data_{db_index}_pkey PRIMARY KEY (key);
            '''),

        "count": "select 'room_data_{db_index}' as table_name, count(*) as count from room_data_{db_index}",
        "truncate": "truncate table room_data_{db_index};",
        "sum": "select sum(count), 'room_data' as table_name from ({sql}) as t;",
    },
    "access": {
        "create": textwrap.dedent('''
            CREATE TABLE public.room_accessed_record_{db_index} (
                key character varying NOT NULL,
                accessed_at timestamp with time zone,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            );

            ALTER TABLE ONLY public.room_accessed_record_{db_index}
                ADD CONSTRAINT room_accessed_record_{db_index}_pkey PRIMARY KEY (key);

            CREATE INDEX room_accessed_at_{db_index}_idx ON public.room_accessed_record_{db_index} USING btree (accessed_at);
        '''),
        "count": "select 'room_accessed_record_{db_index}' as table_name, count(*) as count from room_accessed_record_{db_index}",
        "truncate": "truncate table room_accessed_record_{db_index};",
        "sum": "select sum(count), 'room_access_record' as table_name from ({sql}) as t;",
    },
    "write": {
        "create": textwrap.dedent('''
            CREATE TABLE public.room_written_record_{db_index} (
                key character varying NOT NULL,
                written_at timestamp with time zone,
                created_at timestamp with time zone NOT NULL DEFAULT now()
            );

            ALTER TABLE ONLY public.room_written_record_{db_index}
                ADD CONSTRAINT room_written_record_{db_index}_pkey PRIMARY KEY (key);


            CREATE INDEX room_written_at_{db_index}_idx ON public.room_written_record_{db_index} USING btree (written_at);
            '''),
        "count": "select 'room_written_record_{db_index}' as table_name, count(*) as count from room_written_record_{db_index}",
        "truncate": "truncate table room_written_record_{db_index};",
        "sum": "select sum(count), 'room_written_record' as table_name from ({sql}) as t;",
    }
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
    for i in range(start_index, end_index):
        sql = sql_template.format(db_index=i)
        sqls.append(sql)
    join_str = joins[sql_type]
    final_sql = join_str.join(sqls)
    if sql_type == "sum":
        final_sql = SQL[table][sql_type].format(sql=final_sql)
    final_sql = "\n".join([connct_db_sql, final_sql])
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
        choices=["data", "write", "access"], 
        required=True)
    parser.add_argument("-s", "--start_index", type=int, required=True)
    parser.add_argument("-e", "--end_index", type=int, required=True)
    args = parser.parse_args()
    sql = generate_sql(
        args.database, args.sql, args.table,
        args.start_index, args.end_index)
    print(sql)
