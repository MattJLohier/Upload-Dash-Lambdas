import boto3
import pandas as pd
import io
import json

def lambda_handler(event, context):
    try:
        # Parse input JSON for S3 bucket and object names
        input_bucket = event['input_bucket']
        pivot_object_name = event['pivot_file_key']
        report_object_name = event['report_file_key']
        output_bucket = event['output_bucket']
        output_object_name = event['output_file_key'].replace('.xlsx', '.csv')

        merged_output_file_key = 'merged_data.csv'
        opt_output_file_key = 'opt_output.csv'
        con_output_file_key = 'con_output.csv'
        priv_output_file_key = 'priv_output.csv'
        pub_output_file_key = 'pub_output.csv'
        promo_output_file_key = 'promo_output.csv'

        # Create an S3 client
        s3 = boto3.client('s3')

        # Fetch files from S3 and read content into memory
        pivot_content = io.BytesIO(s3.get_object(Bucket=input_bucket, Key=pivot_object_name)['Body'].read())
        report_content = io.BytesIO(s3.get_object(Bucket=input_bucket, Key=report_object_name)['Body'].read())

        # Read the Excel files with pandas
        df_pivot = pd.read_excel(pivot_content, sheet_name='Product & Pricing Pivot Data', header=3)
        df_report = pd.read_excel(report_content, sheet_name='Product Details', header=5, skiprows=[6])
        df_opt = pd.read_excel(report_content, sheet_name='HW & Option Pricing', header=3, skiprows=[4])
        df_con = pd.read_excel(report_content, sheet_name='Consumables Database', header=3, skiprows=[4])
        df_priv = pd.read_excel(report_content, sheet_name='Private Sector Contract Databas', header=3, skiprows=[4])
        df_pub = pd.read_excel(report_content, sheet_name='Public Sector Contract Database', header=3, skiprows=[4])
        df_promo = pd.read_excel(report_content, sheet_name='Promotions', header=3, skiprows=[4])

        # Set 'Product' column as index for merging
        if 'Product' in df_pivot.columns and 'Product' in df_report.columns:
            df_pivot.set_index('Product', inplace=True)
            df_report.set_index('Product', inplace=True)
        else:
            missing_columns = []
            if 'Product' not in df_pivot.columns:
                missing_columns.append('Product in pivot data')
            if 'Product' not in df_report.columns:
                missing_columns.append('Product in report data')
            raise ValueError(f"Product column not found in: {', '.join(missing_columns)}")

        # Clean and rename columns
        df_pivot.columns = [col.strip().replace(' ', '_') + '_pivot' for col in df_pivot.columns]
        df_report.columns = [col.strip().replace(' ', '_') + '_report' for col in df_report.columns]

        # Merge data
        merged_df = df_pivot.join(df_report, how='inner')
        merged_df.replace(['na', '-'], '', inplace=True)

        df_opt.replace(['na', '-'], '', inplace=True)
        df_con.replace(['na', '-'], '', inplace=True)
        df_priv.replace(['na', '-'], '', inplace=True)
        df_pub.replace(['na', '-'], '', inplace=True)
        df_promo.replace(['na', '-'], '', inplace=True)
        
        # Ensure 'Launch_Date_report' and 'Replacement_Date_report' are in datetime format
        merged_df['Launch_Date_report'] = pd.to_datetime(merged_df['Launch_Date_report'], errors='coerce')
        merged_df['Replacement_Date_report'] = pd.to_datetime(merged_df['Replacement_Date_report'], errors='coerce')

        # Function to write dataframe to CSV and upload to S3
        def upload_to_s3(df, file_name):
            output = io.StringIO()
            df.to_csv(output, index=True)
            output.seek(0)
            s3.put_object(Bucket=output_bucket, Key=file_name, Body=output.getvalue())

        # Write result to CSV files and upload to S3
        upload_to_s3(merged_df, merged_output_file_key)
        upload_to_s3(df_opt, opt_output_file_key)
        upload_to_s3(df_con, con_output_file_key)
        upload_to_s3(df_priv, priv_output_file_key)
        upload_to_s3(df_pub, pub_output_file_key)
        upload_to_s3(df_promo, promo_output_file_key)

        return {
            'statusCode': 200,
            'body': json.dumps('All dataframes were successfully exported and uploaded.')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
