from pyspark.sql import DataFrame

def get_result_dataframe(df_product: DataFrame,
                         df_cat: DataFrame,
                         df_product_cat: DataFrame) -> DataFrame:
    """
    Function to get dataframe of product and category names
    (and product names without category) from product, category
    and relationship dataframes. Dataframes must contains
    specific columns to correct work: 'id' and 'name' for
    product dataframe; 'id' and 'name' for category dataframe.
    :param df_product: dataframe of products (must contains 'id' and 'name' columns)
    :param df_cat: dataframe of categories (must contains 'id' and 'name' columns)
    :param df_product_cat: dataframe of product and category relationships
    :return: dataframe of product and category names (and product names without category)
    """
    return df_product.join(
        other=df_product_cat, on=df_product.id == df_product_cat.product_id, how='left'
    ).join(
        other=df_cat, on=df_product_cat.cat_id == df_cat.id, how='left'
    ).select(df_product.name, df_cat.name)