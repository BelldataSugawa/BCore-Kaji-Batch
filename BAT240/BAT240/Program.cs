using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;


namespace BAT240
{
    internal class BAT240
    {
        static int Main(string[] args)
        {
            //*******************************************************************************************
            //2-1 変数定義
            //*******************************************************************************************       
            string RnkStatus = "1";                                         //連携ステータス
            string RnkDate = DateTime.Now.ToString("yyyyMMddHHmmss");       //日付
            string CsvFile = "";                                            //CSVファイル           
            string WorkPath = "";                                           //ワークパス           
            string OutPath = "";                                            //出力先パス


            //CSVテーブル
            DataTable CsvTable = new DataTable();
            CsvTable.Columns.Add("NOCODE-NOCDE1", typeof(string));          //コード番号+枝番
            CsvTable.Columns.Add("WorKFlag", typeof(string));               //完成フラグ

            //採番マスタより最終BR連携Noを取得し、BR連携Noを作成するための変数
            string CMSKBT = "";                                             //番号識別
            int intNOSEQ = 0;                                               //数値型SEQNo
            string NOSEQ = "";                                              //文字列型SEQNo
            int intCMSAIB = 0;                                              //数値型最終BR連携No
            string CMSAIB = "";                                             //文字列型BR連携No

  
            //*********************************************************************************************
            //2-2汎用コードマスタMKUBNよりデータを取得し、csvファイル名、ワークパス、出力先パスにセット
            //*********************************************************************************************
            //接続文字列の作成
            string connStr = ConfigurationManager.ConnectionStrings["SQLconnect"].ConnectionString;

            //DB接続
            try
            {

                using (var conn = new OdbcConnection(connStr))
                {
                    conn.Open();
                    Console.WriteLine("接続成功");
                    var cmd = new OdbcCommand("SELECT CDKUBN, VLKUBN, TXMJT1, TXMJT2  FROM MKUBN where (CDKUBN = 'TXCDRN' and VLKUBN IN ('1', '2')) or (CDKUBN = 'KBBRRN' and  VLKUBN ='5')", conn);


                    //データの取得
                    var KBNread = cmd.ExecuteReader();


                    //取得したデータが０件の場合
                    if (!KBNread.HasRows)
                    {
                        Console.WriteLine("対象データがありません。");
                        Console.WriteLine("継続するには何かキーを入れてください");
                        return 0;

                    }

                    while (KBNread.Read())
                    {
                        //取得したデータを変数にセット
                        string CDKUBN = KBNread["CDKUBN"].ToString();
                        string VLKUBN = KBNread["VLKUBN"].ToString();
                        string TXMJT1 = KBNread["TXMJT1"].ToString();
                        string TXMJT2 = KBNread["TXMJT2"].ToString();

                        //取得したデータを変数出力
                        Console.WriteLine($"2-2====CDKUBN:{CDKUBN}, VLKUBN:{VLKUBN}, TXMJT1:{TXMJT1}, TXMJT2:{TXMJT2}");

                        //csvファイル名、ワークパス、出力先パスにセット
                        switch (CDKUBN)
                        {
                            //CDKUBNがKBBRRNの場合、EnvModeがPRODUCTIONなら出力先パスにTXMJT1をセット、それ以外の場合TXMJT2をセットする
                            case "KBBRRN":
                                if (ConfigurationManager.AppSettings["EnvMode"] == "PRODUCTION")
                                {
                                    OutPath = TXMJT1;
                                }
                                else
                                {
                                    OutPath = TXMJT2; 
                                }
                                OutPath = OutPath.Replace('¥', '\\');           //DBから取得した後に円マーク(165)をバックスラッシュ(92)に置換
                                OutPath = OutPath.Replace('/', '\\');
                                break;

                            //CDKUBNがTXCDRNの場合、VLKUBNが１の時csvファイル名にTXMJT1をセット
                            case "TXCDRN":
                                if (VLKUBN == "1")
                                    CsvFile = ($"{TXMJT1}{RnkDate}.csv");
                                else
                                {
                                    //VLKUBNが１以外の時、EnvModeがPRODUCTIONならワークパスにTXMJT1をセット、それ以外の場合TXMJT2をセットする
                                    if (ConfigurationManager.AppSettings["EnvMode"] == "PRODUCTION")
                                    {
                                        WorkPath = TXMJT1;
                                    }
                                    else
                                    {
                                        WorkPath = TXMJT2;
                                    }
                                }
                                WorkPath = WorkPath.Replace('¥', '\\');          //DBから取得した後に円マーク(165)をバックスラッシュ(92)に置換
                                WorkPath = WorkPath.Replace('/', '\\');
                                break;
                        }
                    }
                    Console.WriteLine($"出力先パス:{OutPath}, Csvファイル:{CsvFile}, ワークパス:{WorkPath}");
                }
            }
            catch (OdbcException ex)
            {
                Console.WriteLine($"DB接続エラー: {ex.Message}");
                Console.WriteLine("継続するには何かキーを入れてください");
                return 9;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"エラー: {ex.Message}");
                Console.WriteLine("継続するには何かキーを入れてください");
                return 9;
            }


            //*********************************************************************************************
            //2-3  BaseRightへ未送信の情報を取得
            //*********************************************************************************************
            //未送信データの取得
            var FHZYNreturn = GetData();


            //DB接続エラー、エラーの場合、戻り値を返す
            if (FHZYNreturn == null)
            {
                Console.WriteLine("継続するには何かキーを入れてください");
                return 0;
            }

            //DBから取得したデータをFHZYNFileのリストで受け取る
            List<FHZYNFile> Flist = GetData();

            //取得したデータの内容を出力
            foreach (var item in Flist)
            {
                Console.WriteLine($"2-3===== NOCODE:{item.NOCODE}, NOCDE1:{item.NOCDE1}, NOYSRV:{item.NOYSRV}, KBSTKN:{item.KBSTKN}, KBBRNK: {item.KBBRKN}, CDSEIB:{item.CDSEIB}, FGNEW:{item.FGNEW}");
            }

            //*********************************************************************************************
            //2-4-1 取得したデータ+完了フラグ1件ごとにCSVテーブルにBaseRight連携データを出力
            //*********************************************************************************************
            //未送信データの有無チェック
            if (Flist.Count == 0)                 //未送信データ0の場合戻り値0を返す
            {
                Console.WriteLine("対象データがありません。");
                Console.WriteLine("継続するには何かキーを入れてください");
                return 0;
            }
            else          //未送信データ有の場合、完成フラグを追加する
            {
                int count = 0;

                //採番マスタより最終BR連携Noを取得し、BR連携Noを作成する
                try
                {
                    using (var conn = new OdbcConnection(connStr))
                    {
                        //DB接続
                        conn.Open();
                        Console.WriteLine("接続成功");

                        //採番マスタより最終BR連携Noの取得
                        string SelectSql2 = "SELECT CMSKBT, NOSEQ, CMSAIB FROM CMSABN where CMSKBT = 'NOBRRN' ORDER BY CMSAIB DESC LIMIT 1";
                        var cmd1 = new OdbcCommand(SelectSql2, conn);
                        var CMSABNread = cmd1.ExecuteReader();

                        //最終BR連携Noが取得できた場合、番号識別、SEQNo、最終BR連携Noを変数にセットし、出力する。
                        if (CMSABNread.Read())
                        {
                            //最終BR連携Noを出力
                            CMSKBT = CMSABNread["CMSKBT"].ToString();
                            intNOSEQ = int.Parse(CMSABNread["NOSEQ"].ToString());
                            intCMSAIB = int.Parse(CMSABNread["CMSAIB"].ToString().Trim());
                            Console.WriteLine($"番号識別:{CMSKBT}, SEEQNo:{intNOSEQ},最終BR連携No:{intCMSAIB}");

                            //BR連携Noの作成
                            NOSEQ = (intNOSEQ + 1).ToString();
                            CMSAIB = (intCMSAIB + 1).ToString();
                        }
                        //最終BR連携Noが取得できない場合、番号識別にNOBRRN、SEQNoに1、BR連携Noに1をセットし、出力する。
                        else
                        {
                            Console.WriteLine("CMSABNからデータが取得できませんでした。");
                            CMSKBT = "NOBRRN";
                            NOSEQ = "1";
                            CMSAIB = "1";
                        }
                        string insertSql1 = "INSERT INTO CMSABN (CMSKBT, NOSEQ, CMSAIB) VALUES (?,?,?)";
                        var cmd2 = new OdbcCommand(insertSql1, conn);
                        cmd2.Parameters.Add("CMSKBT", OdbcType.VarChar).Value = "NOBRRN";
                        cmd2.Parameters.Add("NOSEQ", OdbcType.Int).Value = NOSEQ;
                        cmd2.Parameters.Add("CMSAIB", OdbcType.Int).Value = CMSAIB;

                        //追加するCMSABNの内容を出力
                        Console.WriteLine($"追加するCMSABNの内容: 番号識別:{CMSKBT}, SEEQNo:{NOSEQ}, BR連携No:{CMSAIB}");

                        //cmd2.ExecuteNonQuery();　　　// 実行
                    }
                }
                catch (OdbcException ex)
                {
                    Console.WriteLine($"DB接続エラー: {ex.Message}");
                    Console.WriteLine("継続するには何かキーを入れてください");
                    return 9;            //戻り値9を返す
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"エラー: {ex.Message}");
                    Console.WriteLine("継続するには何かキーを入れてください");
                    return 9;            //戻り値9を返す
                }


                //完成フラグの追加
                foreach (var item in Flist)
                {
                    count++;
                    //完成フラグの判断
                    //KBSTKNが010の場合、WorkFlagに0をセット
                    if (item.KBSTKN == "010")
                    {
                        item.WorkFlag = "";
                    }

                    //KBSTKNが020の場合、WorkFlagに1をセット
                    else if (item.KBSTKN == "020")
                    {
                        item.WorkFlag = "1";
                    }

                    Console.WriteLine($"2-4-1WorkFlag追加====コード番号:{item.NOCODE},枝番:{item.NOCDE1}, 完了フラグ:{item.WorkFlag}");

                    //*********************************************************************************************
                    //2-4-2 CSVテーブルにコード番号と完成フラグを追加
                    //*********************************************************************************************
                    CsvTable.Rows.Add($"{item.NOCODE}-{item.NOCDE1}", $"{item.WorkFlag}");

                    //CSVテーブルの内容を出力
                    foreach (DataRow row in CsvTable.Rows)
                    {
                        // 列名 (ColumnName) と データ型 (DataType) を出力
                        Console.WriteLine(string.Join(", ", row.ItemArray));
                    }

                    //*********************************************************************************************
                    //2-4-3　　ファイル更新処理
                    //*********************************************************************************************
                    //BaseRight連携履歴明細ファイル(コード番号)の追加
                    try
                    {
                        using (var conn = new OdbcConnection(connStr))
                        { 
                            conn.Open();
                            Console.WriteLine("接続成功");
                            string insertSql2 = "INSERT INTO FBBRC (SPIPGM, NOBRRN, NPBRRH, NOCODE, FGKANR) VALUES (?, ?, ?, ?, ?)";
                            var cmd = new OdbcCommand(insertSql2, conn);

                            cmd.Parameters.Add("SPIPGM", OdbcType.VarChar).Value = "BAT240";
                            cmd.Parameters.Add("NOBRRN", OdbcType.VarChar).Value = CMSAIB;
                            cmd.Parameters.Add("NPBRRH", OdbcType.Int).Value = count;
                            cmd.Parameters.Add("NOCODE", OdbcType.VarChar).Value = item.NOCODE;
                            cmd.Parameters.Add("FGKANR", OdbcType.VarChar).Value = item.WorkFlag;

                            Console.WriteLine($"追加するFBBRCの内容: プログラムID:BAT240, BR連携No:{CMSAIB}, BR連携品目行No:{count}, コード番号:{item.NOCODE}, 完了フラグ:{item.WorkFlag}");

                            //cmd.ExecuteNonQuery();　　　// 実行
                        }
                    }
                    catch (OdbcException ex)
                    {
                        Console.WriteLine($"DB接続エラー: {ex.Message}");
                        Console.WriteLine("継続するには何かキーを入れてください");
                        return 9;            //戻り値9を返す
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"エラー: {ex.Message}");
                        Console.WriteLine("継続するには何かキーを入れてください");
                        return 9;            //戻り値9を返す
                    }

                    //実行予算見出しファイルの更新
                    try
                    {
                        using (var conn = new OdbcConnection(connStr))
                        {
                            conn.Open();
                            Console.WriteLine("接続成功");
                            string UpdateSql1 = "UPDATE FHZYN SET SPIPGM = ?, KBBRKN = ? WHERE NOCODE = ? and NOCDE1 = ? and NOYSRV = ?";
                            var cmd = new OdbcCommand(UpdateSql1, conn);

                            cmd.Parameters.Add("SPIPGM", OdbcType.VarChar).Value = "BAT240";
                            cmd.Parameters.Add("KBBRKN", OdbcType.VarChar).Value = item.KBSTKN;
                            cmd.Parameters.Add("NOCODE", OdbcType.VarChar).Value = item.NOCODE;
                            cmd.Parameters.Add("NOCDE1", OdbcType.VarChar).Value = item.NOCDE1;
                            cmd.Parameters.Add("NOYSRV", OdbcType.Int).Value = item.NOYSRV;

                            Console.WriteLine($"更新するFHZYNの内容: プログラムID:BAT240, BR連携完了状況:{item.KBSTKN}, コード番号:{item.NOCODE}, 枝番:{item.NOCDE1}, リビジョン:{item.NOYSRV}");

                            //cmd.ExecuteNonQuery();    // 実行
                        }
                    }
                    catch (OdbcException ex)
                    {
                        Console.WriteLine($"DB接続エラー: {ex.Message}");
                        Console.WriteLine("継続するには何かキーを入れてください");
                        return 9;     
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"エラー: {ex.Message}");
                        Console.WriteLine("継続するには何かキーを入れてください");
                        return 9;            
                    }
                }
            }
            //*********************************************************************************************
            //2-5　CSVテーブルからワークパス上に、CSVファイルを作成
            //*********************************************************************************************
            //ワークパス上にCSVファイルを作成

            try
            {
                if (!Directory.Exists(WorkPath))
                {
                    Directory.CreateDirectory(WorkPath);                 //WorkPathフォルダが存在しない場合、作成
                }

                if (File.Exists(CsvFile))
                {
                    File.Delete(CsvFile);                                 //同名のCSVファイルが存在する場合、削除
                }
                string FromPath = Path.Combine(WorkPath, CsvFile);        //WorkPathフォルダの中にCsvFileパスを作成



                //CSVテーブルをCSVファイルに書き込み           
                using (var writer = new StreamWriter(FromPath, append: false, encoding: Encoding.UTF8))
                {
                    //  CSVのヘッダー行を書き込み
                    writer.WriteLine("コード番号,完了フラグ");
                    foreach (DataRow row in CsvTable.Rows)
                    {
                        // カンマで結合して1行書き込み
                        writer.WriteLine(string.Join(",", row.ItemArray));
                    }
                }


                //ファイル作成場所の出力
                Console.WriteLine("ファイル作成場所: " + FromPath);

                //作成したCSVファイルの内容を出力
                try
                {
                    string[] lines = File.ReadAllLines(FromPath);
                    foreach (string line in lines)
                    {
                        Console.WriteLine($"2-5====={line}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"CSVファイル読み込めない: {ex.Message}");

                }
                //*********************************************************************************************
                //2-6　CSVファイルを移送する
                //*********************************************************************************************
                if (!Directory.Exists(OutPath))
                {
                    Directory.CreateDirectory(OutPath);              //OutPathフォルダが存在しない場合、OutPathフォルダ作成
                }

                string Topath = Path.Combine(OutPath, CsvFile);     //OutPathフォルダの中にCsvFileパスを作成
                if (!Directory.Exists(Topath))
                {
                    File.Delete(Topath);                                //同名のCSVファイルが存在する場合、削除
                }
                File.Move(FromPath, Topath);                            //CSVファイルをWorkPathからOutPathに移動
                Console.WriteLine($"CSVファイルを{FromPath}から{Topath}に移動しました。");
            }
            catch (IOException ex)
            {
                Console.WriteLine($"CSVファイルエラー: {ex.Message}");
                Console.WriteLine("継続するには何かキーを入れてください");
                return 9;           
            }
            catch (Exception ex)
            {
                Console.WriteLine($"エラー: {ex.Message}");
                Console.WriteLine("継続するには何かキーを入れてください");
                return 9;            
            }

            //*********************************************************************************************
            //2-7　BaseRight連携履歴見出ファイルの追加
            //*********************************************************************************************
            try
            {
                using (var conn = new OdbcConnection(connStr))
                {
                    conn.Open();
                    Console.WriteLine("接続成功");
                    string insertSql3 = "INSERT INTO FHBRR (SPIPGM, NOBRRN, KBBRRN, DTBRRN, STRENK, TXCOMT, VLPTBF, VLPTAF) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                    var cmd = new OdbcCommand(insertSql3, conn);

                    cmd.Parameters.Add("SPIPGM", OdbcType.VarChar).Value = "BAT240";
                    cmd.Parameters.Add("NOBRRN", OdbcType.VarChar).Value = CMSAIB;
                    cmd.Parameters.Add("KBBRRN", OdbcType.Int).Value = "5";
                    cmd.Parameters.Add("DTBRRN", OdbcType.VarChar).Value = RnkDate;
                    cmd.Parameters.Add("STRENK", OdbcType.VarChar).Value = RnkStatus;
                    cmd.Parameters.Add("TXCOMT", OdbcType.VarChar).Value = "";
                    cmd.Parameters.Add("VLPTBF", OdbcType.VarChar).Value = Path.Combine(OutPath, CsvFile);
                    cmd.Parameters.Add("VLPTAF", OdbcType.VarChar).Value = "";

                    Console.WriteLine($"追加するFHBRRの内容: プログラムID:BAT240, BR連携No:{CMSAIB}, BR連携区分:5, BR連携日時:{RnkDate}, ステータス:{RnkStatus}, コメント:'', 出力元パス:{Path.Combine(OutPath, CsvFile)}, 出力先パス:''");

                    //cmd.ExecuteNonQuery();　　　// 実行
                }
                Console.WriteLine("継続するには何かキーを入れてください");
                return 0;   //正常終了
            }
            catch (OdbcException ex)
            {
                Console.WriteLine($"DB接続エラー: {ex.Message}");
                Console.WriteLine("継続するには何かキーを入れてください");
                return 9;            //戻り値9を返す
            }
            catch (Exception ex)
            {
                Console.WriteLine($"エラー: {ex.Message}");
                Console.WriteLine("継続するには何かキーを入れてください");
                return 9;            //戻り値9を返す
            }
        }





        //*********************************************************************************************
        //2-2　汎用コードマスタMKUBNのデータ取得リスト内容
        //*********************************************************************************************
        public class KBNMaster
        {
            public string CDKUBN { get; set; }　　//区分識別コード
            public string VLKUBN { get; set; }    //区分値
            public string TXMJT1 { get; set; }  　//文字値１
            public string TXMJT2 { get; set; }　　//文字値２
        }

        //*********************************************************************************************
        //2-3　BaseRightへ未送信の情報を取得
        //*********************************************************************************************
        //リスト内容
        public class FHZYNFile
        {
            public string NOCODE { get; set; }　　//コード番号
            public string NOCDE1 { get; set; }　　//枝番
            public string NOYSRV { get; set; }　　//実行予算リビジョン  
            public string KBBRKN { get; set; }    //BaseRight送信完了状況
            public string KBSTKN { get; set; }　　//完了状況
            public string CDSEIB { get; set; }　　//区分識別
            public string FGNEW { get; set; }　　 //有効フラグ
            public string WorkFlag { get; set; }　//WK完成フラグ
        }

        //未送信データの取得
        static List<FHZYNFile> GetData()
        {
            //DBからデータを取得し、FHZYNFileのリストで返す
            var Flist = new List<FHZYNFile>();
            //接続文字列の作成
            string connStr = ConfigurationManager.ConnectionStrings["SQLconnect"].ConnectionString;
            //DB接続
            try
            {
                using (var conn = new OdbcConnection(connStr))
                {
                    conn.Open();
                    Console.WriteLine("接続成功");
                    var cmd = new OdbcCommand("SELECT NOCODE, NOCDE1, NOYSRV, KBBRKN, KBSTKN, CDSEIB, FGNEW  FROM FHZYN where (KBBRKN != KBSTKN) and (CDSEIB IN ('H', 'K')) and (FGNEW = '1') order by NOCODE, NOCDE1, NOYSRV ASC", conn);
                    //データの取得
                    var FHZYNread = cmd.ExecuteReader();


                    //取得したデータをFHZYNFileのリストにセット
                    while (FHZYNread.Read())
                    {
                        Flist.Add(new FHZYNFile
                        {
                            NOCODE = FHZYNread["NOCODE"].ToString(),
                            NOCDE1 = FHZYNread["NOCDE1"].ToString(),
                            NOYSRV = FHZYNread["NOYSRV"].ToString(),
                            KBBRKN = FHZYNread["KBBRKN"].ToString(),
                            CDSEIB = FHZYNread["CDSEIB"].ToString(),
                            FGNEW = FHZYNread["FGNEW"].ToString(),
                            KBSTKN = FHZYNread["KBSTKN"].ToString()
                        });
                    }
                    return Flist;
                }
            }
            catch (OdbcException ex)
            {
                Console.WriteLine($"DB接続エラー: {ex.Message}");
                return null;            //nullを返す
            }
            catch (Exception ex)
            {
                Console.WriteLine($"エラー: {ex.Message}");
                return null;            //nullを返す
            }
        }
    }
}

