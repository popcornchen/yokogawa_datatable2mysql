using System;
using System.IO;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Threading;
using System.Collections;
using MySql.Data.MySqlClient;
using System.Data.SqlClient;

namespace demo
{
    class test
    {
        /// <summary>
        ///大批量数据插入,返回成功插入行数
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="table">数据表</param>
        /// <returns>返回成功插入行数</returns>
        /// 

        public string connectionString = "server=localhost; user=root; database=test; port=3306; pwd=angel070711";
        DataTable tb;
        
        
        public DataTable CreatTable()
        {
            tb = new DataTable("datatable_test");
            DataColumn dc = tb.Columns.Add("number", Type.GetType("System.Int32"));
            dc.AutoIncrement = true;//自动增加
            dc.AutoIncrementSeed = 1;//起始为1
            dc.AutoIncrementStep = 1;//步长为1
            dc.AllowDBNull = false;//

            tb.Columns.Add("current1", Type.GetType("System.Double"));
            tb.Columns.Add("voltage1", Type.GetType("System.Double"));

            for (int i = 0; i < 50; i++)
            {
                DataRow newRow = tb.NewRow();
                newRow["current1"] = 12.356;
                newRow["voltage1"] = 88.35;
                tb.Rows.Add(newRow);
            }

            return tb;
        }
        public int BulkInsert(DataTable table)
        {
            MySqlConnection GetConnection = new MySqlConnection(connectionString);
            if (string.IsNullOrEmpty(table.TableName)) throw new Exception("请给DataTable的TableName属性附上表名称");
            if (table.Rows.Count == 0) return 0;
            int insertCount = 0;
            string tmpPath = Path.GetTempFileName();
            string csv = DataTableToCsv(table);
            StreamWriter sw = new StreamWriter(tmpPath, false, UTF8Encoding.UTF8);  //要与mysql的编码方式对象, 数据库要utf8, 表也一样
            sw.Write(csv);
            sw.Close();
            //  File.WriteAllText(tmpPath, csv);
            using (MySqlConnection conn = GetConnection)
            {
                MySqlTransaction tran = null;
                try
                {
                    conn.Open();
                    string command_init = "Truncate table datatable_test";
                    MySqlCommand delete = new MySqlCommand(command_init, conn);
                    delete.ExecuteNonQuery(); 
                    Console.WriteLine("connected");
                    tran = conn.BeginTransaction();
                    MySqlBulkLoader bulk = new MySqlBulkLoader(conn)
                    {
                        FieldTerminator = ",",
                        FieldQuotationCharacter = '"',
                        EscapeCharacter = '"',
                        LineTerminator = "\r\n",
                        FileName = tmpPath,
                        NumberOfLinesToSkip = 0,
                        TableName = table.TableName,    //也是mysql内表的名
                    };
                    //  bulk.CharacterSet = "utf-8";
                    bulk.Columns.AddRange(table.Columns.Cast<DataColumn>().Select(colum => colum.ColumnName).ToList());
                    insertCount = bulk.Load();
                    tran.Commit();
                }
                catch (MySqlException ex)
                {
                    if (tran != null) tran.Rollback();
                    throw ex;
                }
            }
            File.Delete(tmpPath);
            return insertCount;
        }

        ///将DataTable转换为标准的CSV  
        /// </summary>  
        /// <param name="table">数据表</param>  
        /// <returns>返回标准的CSV</returns>  
        private static string DataTableToCsv(DataTable table)
        {
            //以半角逗号（即,）作分隔符，列为空也要表达其存在。  
            //列内容如存在半角逗号（即,）则用半角引号（即""）将该字段值包含起来。  
            //列内容如存在半角引号（即"）则应替换成半角双引号（""）转义，并用半角引号（即""）将该字段值包含起来。  
            StringBuilder sb = new StringBuilder();
            DataColumn colum;
            foreach (DataRow row in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    colum = table.Columns[i];
                    if (i != 0) sb.Append(",");
                    if (colum.DataType == typeof(string) && row[colum].ToString().Contains(","))
                    {
                        sb.Append("\"" + row[colum].ToString().Replace("\"", "\"\"") + "\"");
                    }
                    else sb.Append(row[colum].ToString());
                }
                sb.AppendLine();
            }
            return sb.ToString();
        }
    }
}










namespace yokogawa_datatable2mysql
{
    class Program
    {
        static void Main(string[] args)
        {
            demo.test obj = new demo.test();
            DataTable target = obj.CreatTable();
            obj.BulkInsert(target);
            Console.ReadLine();
        }
    }
}
