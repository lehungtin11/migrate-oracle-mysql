using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Oracle.ManagedDataAccess.Client;
using Dapper;
using Microsoft.VisualBasic.FileIO;
using MySql.Data.MySqlClient;
using MySqlX.XDevAPI.Relational;
using CsvHelper;
using CsvHelper.Configuration;

class Program
{
    static void Main()
    {
        migrate();
    }

    public static void migrate()
    {
        // Replace with your actual Oracle and MySQL connection strings
        string oracleConnectionString = "data source=//localhost:1521/pgdb;user id=root;password=123;";
        string mysqlConnectionString = "server=localhost;port=3306;database=db;uid=root;pwd=123;";
        int countRS = 0;

        // Your Oracle query
        string oracleQuery = @"
            SELECT c.id, c.CASE_NUMBER as Ma, c.PRIORITY as UuTien, c.STATUS as TinhTrang, 
                   c.NAME as ChuDe, cstm.RECEIVE_FROM_C as TiepNhanTu, cstm.TYPE_C as PhanLoai, 
                   cstm.DOI_TUONG_YEU_CAU_C as DoiTuongYeuCau, cstm.MA_CHI_NHANH_C as MaChiNhanh, 
                   cstm.EMAIL_NGUOI_GUI_C as EmailNguoiGui, c.DESCRIPTION as NoiDungPhanHoi, 
                   c.RESOLUTION as HuongGiaiQuyet, cstm.CUSTOMER_ESTIMATE_C as KhachHangDanhGia, 
                   cstm.CUSTOMER_ESTIMATE_DETAIL_C as YKienKhachHang, c.ASSIGNED_USER_ID as NguoiPhuTrach, 
                   (SELECT USER_NAME FROM ORA.USERS WHERE ID = c.ASSIGNED_USER_ID) as NguoiPhuTrach_username, 
                   c.ACCOUNT_NAME as TenKhachhang, cstm.REQUESTED_USER_ID_C as NguoiYeuCauNoiBo, 
                   (SELECT USER_NAME FROM ORA.USERS WHERE ID = cstm.REQUESTED_USER_ID_C) as NguoiYeuCauNoiBo_username, 
                   c.DATE_MODIFIED as NgaySuaCuoi, c.DATE_ENTERED as NgayTao, cstm.CATEGORY_C as LinhVuc, 
                   cstm.SUB_CATEGORY_C as LinhVucChiTiet, c.ACCOUNT_ID as MaKhachHang, acc.CIF_C as CIF, acc.REGISTER_MOBILE_C as SDT_DANGKY 
            FROM ORA.CASES c 
            JOIN ORA.CASES_CSTM cstm ON c.ID = cstm.ID_C
            LEFT JOIN ORA.CRMV_ACCOUNTS_CSTM acc ON acc.ID_C = c.ACCOUNT_ID";

        try
        {
            using (OracleConnection oracleConnection = new OracleConnection(oracleConnectionString))
            {
                oracleConnection.Open();
                using (OracleCommand oracleCommand = new OracleCommand(oracleQuery, oracleConnection))
                using (OracleDataReader reader = oracleCommand.ExecuteReader())
                {
                    using (MySqlConnection mysqlConnection = new MySqlConnection(mysqlConnectionString))
                    {
                        mysqlConnection.Open();
                        while (reader.Read())
                        {
                            string insertCommandText = @"
                                INSERT INTO end_data 
                                (ID, MA, UUTIEN, TINHTRANG, CHUDE, TIEPNHANTU, PHANLOAI, DOITUONGYEUCAU, MACHINHANH, EMAILNGUOIGUI, 
                                 NOIDUNGPHANHOI, HUONGGIAIQUYET, KHACHHANGDANHGIA, YKIENKHACHHANG, NGUOIPHUTRACH, 
                                 NGUOIPHUTRACH_USERNAME, TENKHACHHANG, NGUOIYEUCAUNOIBO, NGUOIYEUCAUNOIBO_USERNAME, NGAYSUACUOI, 
                                 NGAYTAO, LINHVUC, LINHVUCCHITIET, MAKHACHHANG, SDT_DANGKY, CIF) 
                                VALUES (@ID, @MA, @UUTIEN, @TINHTRANG, @CHUDE, @TIEPNHANTU, @PHANLOAI, @DOITUONGYEUCAU, @MACHINHANH, 
                                        @EMAILNGUOIGUI, @NOIDUNGPHANHOI, @HUONGGIAIQUYET, @KHACHHANGDANHGIA, @YKIENKHACHHANG, 
                                        @NGUOIPHUTRACH, @NGUOIPHUTRACH_USERNAME, @TENKHACHHANG, @NGUOIYEUCAUNOIBO, 
                                        @NGUOIYEUCAUNOIBO_USERNAME, @NGAYSUACUOI, @NGAYTAO, @LINHVUC, @LINHVUCCHITIET, @MAKHACHHANG, @SDT_DANGKY, @CIF)";

                            using (MySqlCommand mysqlCommand = new MySqlCommand(insertCommandText, mysqlConnection))
                            {
                                // Mapping the data from Oracle to MySQL
                                mysqlCommand.Parameters.AddWithValue("@ID", reader["id"]);
                                mysqlCommand.Parameters.AddWithValue("@MA", reader["Ma"]);
                                mysqlCommand.Parameters.AddWithValue("@UUTIEN", reader["UuTien"]);
                                mysqlCommand.Parameters.AddWithValue("@TINHTRANG", reader["TinhTrang"]);
                                mysqlCommand.Parameters.AddWithValue("@CHUDE", reader["ChuDe"]);
                                mysqlCommand.Parameters.AddWithValue("@TIEPNHANTU", reader["TiepNhanTu"]);
                                mysqlCommand.Parameters.AddWithValue("@PHANLOAI", reader["PhanLoai"]);
                                mysqlCommand.Parameters.AddWithValue("@DOITUONGYEUCAU", reader["DoiTuongYeuCau"]);
                                mysqlCommand.Parameters.AddWithValue("@MACHINHANH", reader["MaChiNhanh"]);
                                mysqlCommand.Parameters.AddWithValue("@EMAILNGUOIGUI", reader["EmailNguoiGui"]);
                                mysqlCommand.Parameters.AddWithValue("@NOIDUNGPHANHOI", reader["NoiDungPhanHoi"]);
                                mysqlCommand.Parameters.AddWithValue("@HUONGGIAIQUYET", reader["HuongGiaiQuyet"]);
                                mysqlCommand.Parameters.AddWithValue("@KHACHHANGDANHGIA", reader["KhachHangDanhGia"]);
                                mysqlCommand.Parameters.AddWithValue("@YKIENKHACHHANG", reader["YKienKhachHang"]);
                                mysqlCommand.Parameters.AddWithValue("@NGUOIPHUTRACH", reader["NguoiPhuTrach"]);
                                mysqlCommand.Parameters.AddWithValue("@NGUOIPHUTRACH_USERNAME", reader["NguoiPhuTrach_username"]);
                                mysqlCommand.Parameters.AddWithValue("@TENKHACHHANG", reader["TenKhachhang"]);
                                mysqlCommand.Parameters.AddWithValue("@NGUOIYEUCAUNOIBO", reader["NguoiYeuCauNoiBo"]);
                                mysqlCommand.Parameters.AddWithValue("@NGUOIYEUCAUNOIBO_USERNAME", reader["NguoiYeuCauNoiBo_username"]);
                                mysqlCommand.Parameters.AddWithValue("@NGAYSUACUOI", reader["NgaySuaCuoi"]);
                                mysqlCommand.Parameters.AddWithValue("@NGAYTAO", reader["NgayTao"]);
                                mysqlCommand.Parameters.AddWithValue("@LINHVUC", reader["LinhVuc"]);
                                mysqlCommand.Parameters.AddWithValue("@LINHVUCCHITIET", reader["LinhVucChiTiet"]);
                                mysqlCommand.Parameters.AddWithValue("@MAKHACHHANG", reader["MaKhachHang"]);
                                mysqlCommand.Parameters.AddWithValue("@SDT_DANGKY", reader["SDT_DANGKY"]);
                                mysqlCommand.Parameters.AddWithValue("@CIF", reader["CIF"]);

                                // Execute the insert command
                                mysqlCommand.ExecuteNonQuery();
                            }
                            countRS += 1;
                            Console.WriteLine(countRS);
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred: " + ex.Message);
        }
    }
}


public class Ticket
{
    public string ID { set; get; }
    public string MA { set; get; }
    public string UUTIEN { set; get; }
    public string TINHTRANG { set; get; }
    public string CHUDE { set; get; }
    public string TIEPNHANTU { set; get; }
    public string PHANLOAI { set; get; }
    public string DOITUONGYEUCAU { set; get; }
    public string MACHINHANH { set; get; }
    public string EMAILNGUOIGUI { set; get; }
    public string NOIDUNGPHANHOI { set; get; }
    public string HUONGGIAIQUYET { set; get; }
    public string KHACHHANGDANHGIA { set; get; }
    public string YKIENKHACHHANG { set; get; }
    public string NGUOIPHUTRACH { set; get; }
    public string NGUOIPHUTRACH_USERNAME { set; get; }
    public string TENKHACHHANG { set; get; }
    public string NGUOIYEUCAUNOIBO { set; get; }
    public string NGUOIYEUCAUNOIBO_USERNAME { set; get; }
    public string NGAYSUACUOI { set; get; }
    public string NGAYTAO { set; get; }
    public string LINHVUC { set; get; }
    public string LINHVUCCHITIET { set; get; }
    public string MAKHACHHANG { set; get; }
}