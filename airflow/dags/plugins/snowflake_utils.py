import os

import pandas as pd
import snowflake.connector
from pyspark.sql import SparkSession

from airflow.exceptions import AirflowFailException

def execute_snowflake_query(
    query: str, snowflake_options: dict, data=None, fetch=False
):
    """
    Snowflake에서 SQL 쿼리를 실행하거나 데이터를 조회하는 함수

    Args:
        query (str): 실행할 SQL 쿼리
        snowflake_options (dict): Snowflake 접속 정보
        data (list, optional): executemany를 사용할 경우 전달할 데이터 리스트
        fetch (bool, optional): SELECT 쿼리 실행 후 데이터를 반환할지 여부

    Returns:
        pd.DataFrame | None: fetch=True인 경우 DataFrame 반환, 그렇지 않으면 None 반환
    """
    print(f"snowflake_opt: {snowflake_options}")

    try:
        conn = snowflake.connector.connect(
            user=snowflake_options["user"],
            password=snowflake_options["password"],
            account=snowflake_options["account"],
            database=snowflake_options["db"],
            schema=snowflake_options["schema"],
            warehouse=snowflake_options["warehouse"],
            role=snowflake_options["role"],
        )
        cur = conn.cursor()

        if data:
            if isinstance(data, list):
                cur.executemany(query, data)
            else:
                cur.execute(query, data)
            conn.commit()

        elif not fetch:
            cur.execute(query)
            conn.commit()

        if fetch:
            result = cur.fetchall()  # 데이터 가져오기
            print("result: " + result)
            if cur.description:  # 컬럼 정보가 존재할 경우에만 DataFrame 생성
                df = pd.DataFrame(
                    result, columns=[
                        desc[0] for desc in cur.description])
            else:
                df = pd.DataFrame()  # 빈 DataFrame 반환
            cur.close()
            conn.close()
            return df

        cur.close()
        conn.close()
        print("Query executed successfully.")
    except Exception as e:
        print(f"Execute_snowflake_query Error: {e}")
        print(f"Query: {query}")
        print(f"Data: {data}")
        raise AirflowFailException("execute query error")


def escape_quotes(value):
    if value is None:
        return "NULL"
    return "'{}'".format(value.replace("'", "''"))
