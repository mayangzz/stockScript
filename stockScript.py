import akshare as ak
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time


def get_all_a_stock_codes():
    """获取全量A股代码和名称，使用字符串补零确保6位代码"""
    stock_info = ak.stock_info_a_code_name()
    # 方法1：使用zfill补零（推荐）
    stock_info["code"] = stock_info["code"].astype(str).str.zfill(6)
    # 方法2：使用format补零（备用）
    # stock_info["code"] = stock_info["code"].apply(lambda x: "{:0>6}".format(x))
    return stock_info[["code", "name"]]


def get_stock_history(symbol, name, start_date, end_date):
    """获取单只股票历史数据（线程安全）"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )
        if not df.empty:
            # 深拷贝避免警告
            df = df.copy()
            # 添加补零后的代码和名称
            df["股票代码"] = "{:0>6}".format(int(symbol))  # 再次确保补零
            df["股票名称"] = name
            # 处理百分号字段
            for col in ["换手率", "涨跌幅"]:
                if col in df.columns:
                    df[col] = df[col].astype(str) + "%"
            return df
    except Exception as e:
        print(f"股票 {symbol} 获取失败: {str(e)[:50]}...")  # 截断错误信息
    return pd.DataFrame()


def main():
    # 参数配置
    start_date = "20250322"  # 开始日期
    end_date = "20250328"  # 结束日期
    output_file = f"A股历史数据_{start_date}_{end_date}_v2.csv"
    max_workers = 20  # 线程数（建议10-20）

    # 1. 获取股票列表（已补零）
    print("正在加载A股代码列表...")
    stock_codes = get_all_a_stock_codes()
    print(f"共获取到 {len(stock_codes)} 只A股（代码已补零）")
    print("示例代码：", stock_codes.head(3).values.tolist())

    # 2. 多线程获取数据
    all_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交任务
        futures = {
            executor.submit(
                get_stock_history,
                row["code"],
                row["name"],
                start_date,
                end_date
            ): row["code"] for _, row in stock_codes.iterrows()
        }

        # 进度条
        with tqdm(total=len(futures), desc="数据获取进度") as pbar:
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if not result.empty:
                        all_data.append(result)
                except Exception as e:
                    print(f"处理异常: {str(e)[:50]}...")
                finally:
                    pbar.update(1)

    # 3. 合并和保存数据
    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)

        # 排序：股票代码（升序）→日期（升序）
        df_final.sort_values(["股票代码", "日期"], ascending=[True, True], inplace=True)

        # 规范列顺序
        base_columns = ["股票代码", "股票名称", "日期"]
        other_columns = [col for col in df_final.columns if col not in base_columns]
        df_final = df_final[base_columns + other_columns]

        # 保存结果
        df_final.to_csv(output_file, index=False, encoding="utf_8_sig")
        print(f"\n数据已保存至: {output_file}")
        print(f"总记录数: {len(df_final):,}")
        print("\n首尾各3条数据预览：")
        print(pd.concat([df_final.head(3), df_final.tail(3)]))
    else:
        print("未获取到有效数据")


if __name__ == "__main__":
    main()