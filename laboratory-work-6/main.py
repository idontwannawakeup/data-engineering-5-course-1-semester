import duckdb


table_name = "ElectricVehiclePopulationData"


def find_amount_of_cars_per_city(data):
    amount_of_cars_per_city_query = f"""
        SELECT city, COUNT(*) AS amount_of_cars
        FROM {table_name}
        GROUP BY city
    """

    return data.query(table_name, amount_of_cars_per_city_query)


def find_most_popuar_cars(data):
    most_popuar_cars_query = f"""
        SELECT CONCAT(make, ' ', model) AS full_model, COUNT(*) AS amount_of_model
        FROM {table_name}
        GROUP BY CONCAT(make, ' ', model)
        ORDER BY COUNT(*) DESC
        LIMIT 3
    """

    return data.query(table_name, most_popuar_cars_query)


def find_most_popular_electric_car_per_postal_code(data):
    # If there are multiple most popular cars per postal_code, then multiple models will be displayed for such postal code
    # (For example, there's exactly 5 TESLA MODEL 3 and exactly 5 CHEVROLET VOLT in some postal_code area
    # and this is the most amount of cars in this region)
    most_popular_electric_car_per_postal_code_query = f"""
        SELECT full_model, postal_code, amount_of_cars
        FROM (
            SELECT sub_table.postal_code,
                CONCAT(sub_table.make, ' ', sub_table.model) AS full_model,
                COUNT(*) AS amount_of_cars,
                (
                    RANK() OVER (PARTITION BY (sub_table.postal_code) ORDER BY COUNT(*) DESC)
                ) AS ranking
            FROM {table_name} AS sub_table
            GROUP BY (sub_table.postal_code, CONCAT(sub_table.make, ' ', sub_table.model))
        )
        WHERE ranking = 1
    """

    return data.query(table_name, most_popular_electric_car_per_postal_code_query)


def find_amount_of_cars_per_year(data):
    amount_of_cars_per_year_query = f"""
        SELECT model_year, COUNT(*) as amount_of_cars
        FROM {table_name}
        GROUP BY model_year
    """

    return data.query(table_name, amount_of_cars_per_year_query)


def write_amount_of_cars_per_year_to_parquet(data):
    amount_of_cars_per_year_query = f"""
        SELECT model_year, COUNT(*) as amount_of_cars
        FROM {table_name}
        GROUP BY model_year
    """

    amount_of_cars_per_year_to_parquet_query = f"""
        COPY ({amount_of_cars_per_year_query}) TO 'cars_per_year_parquet' (FORMAT PARQUET, PARTITION_BY model_year, ALLOW_OVERWRITE true);
    """

    data.query(table_name, amount_of_cars_per_year_to_parquet_query)


def main():
    duckdb_data = duckdb.read_csv("./data/Electric_Vehicle_Population_Data.csv", normalize_names=True)

    find_amount_of_cars_per_city(duckdb_data).show()
    find_most_popuar_cars(duckdb_data).show()
    find_most_popular_electric_car_per_postal_code(duckdb_data).show()
    find_amount_of_cars_per_year(duckdb_data).show()

    write_amount_of_cars_per_year_to_parquet(duckdb_data)


if __name__ == "__main__":
    main()
