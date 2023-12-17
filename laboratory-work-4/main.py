import csv
import psycopg2


def create_tables_if_not_exist(conn):
    cursor = conn.cursor()
    query = """
        CREATE TABLE IF NOT EXISTS Customer (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            address_1 VARCHAR(255),
            address_2 VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            zip_code VARCHAR(10),
            join_date DATE
        );

        CREATE INDEX IF NOT EXISTS idx_customer_id ON Customer(customer_id);

        CREATE TABLE IF NOT EXISTS Product (
            product_id SERIAL PRIMARY KEY,
            product_code VARCHAR(10),
            product_description VARCHAR(255)
        );

        CREATE INDEX IF NOT EXISTS idx_product_id ON Product(product_id);

        CREATE TABLE IF NOT EXISTS Transaction (
            transaction_id VARCHAR(50) PRIMARY KEY,
            transaction_date DATE,
            product_id INT REFERENCES Product(product_id),
            product_code VARCHAR(10),
            product_description VARCHAR(255),
            quantity INT,
            account_id INT REFERENCES Customer(customer_id)
        );

        CREATE INDEX IF NOT EXISTS idx_transaction_id ON Transaction(transaction_id);
        CREATE INDEX IF NOT EXISTS idx_account_id ON Transaction(account_id);
    """

    cursor.execute(query)
    conn.commit()
    cursor.close()


def insert_account_data(data, cursor):
    customer_insert_query = """
        INSERT INTO Customer (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """
    cursor.executemany(customer_insert_query, data)


def insert_product_data(data, cursor):
    product_insert_query = """
        INSERT INTO Product (product_id, product_code, product_description)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """
    cursor.executemany(product_insert_query, data)


def insert_transaction_data(data, cursor):
    transaction_insert_query = """
        INSERT INTO Transaction (transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
    """
    cursor.executemany(transaction_insert_query, data)


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    create_tables_if_not_exist(conn)

    cursor = conn.cursor()

    with open('./data/accounts.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        accounts_data = [tuple(row) for row in reader]

    with open('./data/products.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        product_data = [tuple(row) for row in reader]

    with open('./data/transactions.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)
        transaction_data = [tuple(row) for row in reader]

    insert_account_data(accounts_data, cursor)
    insert_product_data(product_data, cursor)
    insert_transaction_data(transaction_data, cursor)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
