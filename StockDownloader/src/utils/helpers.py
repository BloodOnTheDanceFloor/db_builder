from datetime import datetime


def convert_date_format(date_str, from_format="%Y%m%d", to_format="%Y-%m-%d"):
    """转换日期格式"""
    return datetime.strptime(date_str, from_format).strftime(to_format)


def validate_dataframe(df, required_columns):
    """验证 DataFrame 是否包含所需的列"""
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"DataFrame is missing required columns: {required_columns}")
    return df
