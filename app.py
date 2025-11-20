# app.py
import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Madang DB + DuckDB", page_icon="📚")

# 🔹 여기에 본인 학번/이름 적기
st.title("Madang DB + DuckDB + Streamlit")
st.caption("12243743 정의석")  # TODO: 실제 학번/이름으로 수정

@st.cache_resource
def init_connection():
    """DuckDB 연결을 만들고 CSV 파일들을 테이블로 로드."""
    base_dir = Path(__file__).parent

    # 메모리 DB 사용 (파일 DB로 쓰고 싶으면 database='madang.duckdb' 등으로 변경 가능)
    con = duckdb.connect(database=":memory:")

    # CSV → DuckDB 테이블로 로드
    # 각 CSV는 같은 폴더에 있다고 가정
    con.execute("""
        CREATE OR REPLACE TABLE book AS
        SELECT * FROM read_csv_auto(?, header = TRUE);
    """, [str(base_dir / "Book_madang.csv")])

    con.execute("""
        CREATE OR REPLACE TABLE customer AS
        SELECT * FROM read_csv_auto(?, header = TRUE);
    """, [str(base_dir / "Customer_madang.csv")])

    con.execute("""
        CREATE OR REPLACE TABLE orders AS
        SELECT * FROM read_csv_auto(?, header = TRUE);
    """, [str(base_dir / "Orders_madang.csv")])

    return con

# DuckDB 연결 가져오기
con = init_connection()

# 사이드바 메뉴
mode = st.sidebar.radio(
    "메뉴 선택",
    ["원본 테이블 보기", "예시 쿼리 실행", "직접 SQL 쿼리 써보기"]
)

# 1) 원본 테이블 보기
if mode == "원본 테이블 보기":
    st.subheader("원본 테이블 데이터")

    table_name = st.selectbox(
        "테이블 선택",
        ["book", "customer", "orders"],
        format_func=lambda x: {
            "book": "book (도서 정보)",
            "customer": "customer (고객 정보)",
            "orders": "orders (주문 정보)"
        }[x]
    )

    df = con.execute(f"SELECT * FROM {table_name};").df()
    st.dataframe(df, use_container_width=True)

# 2) 예시 쿼리 실행
elif mode == "예시 쿼리 실행":
    st.subheader("예시 SQL 쿼리")

    example = st.selectbox(
        "예시 선택",
        [
            "1. 전체 주문 내역 (고객 + 책 이름 포함)",
            "2. 고객별 총 구매 금액",
            "3. 출판사별 매출",
        ]
    )

    if example == "1. 전체 주문 내역 (고객 + 책 이름 포함)":
        query = """
        SELECT 
            o.orderid,
            c.name AS customer_name,
            b.bookname,
            o.saleprice,
            o.orderdate
        FROM orders o
        JOIN customer c ON o.custid = c.custid
        JOIN book b ON o.bookid = b.bookid
        ORDER BY o.orderdate;
        """
        st.markdown("**설명:** 주문(orders) + 고객(customer) + 도서(book)을 조인해서 전체 주문 내역을 보여줍니다.")

    elif example == "2. 고객별 총 구매 금액":
        query = """
        SELECT 
            c.custid,
            c.name AS customer_name,
            SUM(o.saleprice) AS total_spent,
            COUNT(*) AS num_orders
        FROM orders o
        JOIN customer c ON o.custid = c.custid
        GROUP BY c.custid, c.name
        ORDER BY total_spent DESC;
        """
        st.markdown("**설명:** 고객별로 총 구매금액과 주문 횟수를 집계합니다.")

    else:  # "3. 출판사별 매출"
        query = """
        SELECT 
            b.publisher,
            SUM(o.saleprice) AS total_sales,
            COUNT(*) AS num_orders
        FROM orders o
        JOIN book b ON o.bookid = b.bookid
        GROUP BY b.publisher
        ORDER BY total_sales DESC;
        """
        st.markdown("**설명:** 출판사별 총 매출과 주문 건수를 계산합니다.")

    st.code(query, language="sql")

    df_result = con.execute(query).df()
    st.dataframe(df_result, use_container_width=True)

# 3) 직접 SQL 쿼리 써보기
else:
    st.subheader("직접 SQL 쿼리 실행")

    default_query = "SELECT * FROM book LIMIT 5;"
    user_query = st.text_area(
        "SQL을 직접 입력하세요 (테이블: book, customer, orders 사용 가능)",
        value=default_query,
        height=150
    )

    if st.button("쿼리 실행"):
        try:
            df_user = con.execute(user_query).df()
            st.success("쿼리 실행 성공!")
            st.dataframe(df_user, use_container_width=True)
        except Exception as e:
            st.error(f"쿼리 실행 중 오류가 발생했습니다: {e}")
