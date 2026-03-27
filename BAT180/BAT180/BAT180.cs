using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static BAT180.BAT180;
using static BAT180.BAT180.DesignChangeFileRepository;
using static BAT180.BAT180.ItemMasterHistoryRepository;
using static BAT180.BAT180.ItemMasterRepository;
using static BAT180.BAT180.ItemMasterStructureRepository;


namespace BAT180
{
    internal class BAT180
    {
        static int Main(string[] args)
        {
            // *********************************************************************************
            // * Mainルーチン
            // *********************************************************************************
            Console.WriteLine("プログラム開始しました。");
            Console.WriteLine("プログラムは中断しています。再開するには何かキーを入力してください");
            string RTN = Console.ReadLine();
            try
            {
                // *********************************************************************************
                // * 変数定義
                // *********************************************************************************
                #region 変数定義
                String registrationProgramId = "BAT180";
                String registrationUserID = "BATCH";
                // 現在日時を取得し、yyyyMMddHHmmss形式で保持
                long integrationDateTime = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmss"));

                //// CSVファイル名
                //string csvFileName_Item = null; // (品目)
                //string csvFileName_ItemStructure = null; // (品目構成)
                //string csvFileName_DesignChange = null; // (設計変更)

                // 連携ステータスコメント
                string integrationStatusComment_Success = null; // (正常終了時)
                string integrationStatusComment_Error = null; // (エラー時)

                // フォルダパス
                string successFolderPath = null; // (正常終了用)
                string errorFolderPath = null; // (エラー時用)
                
                // 連携エラー区分
                bool integrationErrorType_Item = false; // (品目)
                bool integrationErrorType_ItemStructure = false; // (品目構成)
                bool integrationErrorType_DesignChange = false; // (設計変更)

                // 連携ステータス
                String IntegrationErrorSatus = null;

                // 連携元パス
                string sourcePath_Item = null; // (品目)
                string sourcePath_ItemStructure = null; // (品目構成)
                string sourcePath_DesignChange = null; // (設計変更)

                // 連携後フォルダパス（正常終了用）
                string successFolderPath_After_Item = null; // (品目)
                string successFolderPath_After_ItemStructure = null; // (品目構成)
                string successFolderPath_After_DesignChange = null; // (設計変更)

                // 連携後フォルダパス（エラー時用）
                string errorFolderPath_After_Item = null; // (品目)
                string errorFolderPath_After_ItemStructure = null; // (品目構成)
                string errorFolderPath_After_DesignChange = null; // (設計変更)

                // 連携元パス（\をバックスラッシュに置き換え）
                string targetFolderItem = null; //品目
                string targetFolderItemStructure = null; //品目構成
                string targetFolderDesignChange = null; //設計変更

                // 連携後フォルダパス（正常終了用）（\をバックスラッシュに置き換え）
                string successTargetFolderItem = null; //品目
                string successTargetFolderItemStructure = null; //品目構成
                string successTargetFolderDesignChange = null; //設計変更

                // 連携後フォルダパス（エラー時用）（\をバックスラッシュに置き換え）
                string errorTargetFolderItem = null; //品目
                string errorTargetFolderItemStructure = null; //品目構成
                string errorTargetFolderDesignChange = null; //設計変更

                // 連携後コメント
                string afterIntegrationComment_Item = null; // (品目)
                string afterIntegrationComment_ItemStructure = null; // (品目構成)
                string afterIntegrationComment_DesignChange = null; // (設計変更)

                // 不要パラメータ用
                string unnecessary = null;

                // ファイル名プリフィックス
                string filePrefix_Item = "zubanmaster";
                string filePrifix_ItemStructure = "hyoujyunkousei";
                string filePrifix_DesignChange = "setsuhentsuuchi";

                #endregion
                // *********************************************************************************
                // * 引数のチェック
                // *********************************************************************************
                #region 引数のチェック
                if (args.Length != 1)
                {
#if DEBUG
                    Console.WriteLine("エラー: 依頼番号が指定されていません。");
                    Console.WriteLine("戻り値:3");
                    return 3;
#else
                    Environment.Exit(3); // 戻り値として「3」を返して終了
#endif
                }
                string strIraiNo = args[0];
                // 依頼番号の形式をバリデーション
                string pattern = @"^\d{6}-\d{6}$"; // 正規表現パターン
                if (!Regex.IsMatch(strIraiNo, pattern))
                {
#if DEBUG
                    Console.WriteLine("エラー: 依頼番号は9999-9999形式である必要があります。");
                    Console.WriteLine("戻り値:3");
                    return 3;
#else
                    Environment.Exit(3); // 戻り値として「3」を返して終了
#endif
                }

                // 依頼番号出力
                Console.WriteLine($"処理対象の依頼No: {strIraiNo}");

                #endregion
                // *********************************************************************************
                // * データベース接続
                // *********************************************************************************
                string connectionString = ConfigurationManager.ConnectionStrings["IBM_i_ODBC_Connection"].ConnectionString;
                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {

                    connection.Open();
                    Console.WriteLine("接続成功！");
                    // *********************************************************************************
                    // * 環境変数設定
                    // *********************************************************************************
                    #region 環境変数設定
                    string envMode = ConfigurationManager.AppSettings["EnvMode"];

                    // SQLクエリを定義
                    string query = "SELECT VLKUBN, TXMJT1, TXMJT2 FROM MKUBN WHERE CDKUBN = ? AND VLKUBN = ?";

                    // 連携元パスの取得
                    //GetMKUBN(connection, query, envMode, "KBBRRN", "1", 
                    //         out sourcePath_Item , out unnecessary);

                    //GetMKUBN(connection, query, envMode, "KBBRRN", "2", 
                    //         out sourcePath_ItemStructure, out unnecessary);

                    //GetMKUBN(connection, query, envMode, "KBBRRN", "4", 
                    //    out sourcePath_DesignChange, out unnecessary);

                    sourcePath_Item = @"C:\AS400RENKEIBATCH\02RenkeiData\02OutData\01HINBAN";
                    sourcePath_ItemStructure = @"C:\AS400RENKEIBATCH\02RenkeiData\02OutData\01HINBAN";
                    sourcePath_DesignChange = @"C:\AS400RENKEIBATCH\02RenkeiData\02OutData\01HINBAN";

                    // 正常終了用フォルダ、連携ステータスコメント（正常終了時）を取得

                    GetMKUBN(connection, query, null, "STRENK", "1", 
                        out integrationStatusComment_Success, out successFolderPath);

                    // エラー終了用フォルダ、連携ステータスコメント（エラー終了時）を取得

                    GetMKUBN(connection, query, null, "STRENK", "9",
                        out integrationStatusComment_Error, out errorFolderPath);

                    #endregion
                    // *********************************************************************************
                    // * フォルダのバックスラッシュ化
                    // *********************************************************************************
                    #region フォルダのバックスラッシュ化
                    string fixedPath = sourcePath_Item
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    targetFolderItem = Path.GetFullPath(fixedPath);

                    fixedPath = sourcePath_ItemStructure
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    targetFolderItemStructure = Path.GetFullPath(fixedPath);

                    fixedPath = sourcePath_DesignChange
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    targetFolderDesignChange = Path.GetFullPath(fixedPath);


                    // 連携後フォルダパスの設定
                    successFolderPath_After_Item = sourcePath_Item + successFolderPath;
                    successFolderPath_After_ItemStructure = sourcePath_ItemStructure + successFolderPath;
                    successFolderPath_After_DesignChange = sourcePath_DesignChange + successFolderPath;

                    fixedPath = successFolderPath_After_Item
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    successTargetFolderItem = Path.GetFullPath(fixedPath);

                    fixedPath = successFolderPath_After_ItemStructure
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    successTargetFolderItemStructure = Path.GetFullPath(fixedPath);

                    fixedPath = successFolderPath_After_DesignChange
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    successTargetFolderDesignChange = Path.GetFullPath(fixedPath);

                    errorFolderPath_After_Item = sourcePath_Item + errorFolderPath;
                    errorFolderPath_After_ItemStructure = sourcePath_ItemStructure + errorFolderPath;
                    errorFolderPath_After_DesignChange = sourcePath_DesignChange + errorFolderPath;

                    fixedPath = errorFolderPath_After_Item
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    errorTargetFolderItem = Path.GetFullPath(fixedPath);

                    fixedPath = errorFolderPath_After_ItemStructure
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    errorTargetFolderItemStructure = Path.GetFullPath(fixedPath);

                    fixedPath = errorFolderPath_After_DesignChange
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    errorTargetFolderDesignChange = Path.GetFullPath(fixedPath);

                    // デバッグ出力
                    Console.WriteLine($"連携元パス (品目): {sourcePath_Item}");
                    Console.WriteLine($"成功フォルダパス (品目): {successFolderPath_After_Item}");
                    Console.WriteLine($"エラーフォルダパス (品目): {errorFolderPath_After_Item}");
                    //
                    Console.WriteLine($"連携元パス (品目構成): {sourcePath_ItemStructure}");
                    Console.WriteLine($"成功フォルダパス (品目構成): {successFolderPath_After_ItemStructure}");
                    Console.WriteLine($"エラーフォルダパス (品目構成): {errorFolderPath_After_ItemStructure}");
                    //
                    Console.WriteLine($"連携元パス (設計変更): {sourcePath_Item}");
                    Console.WriteLine($"成功フォルダパス (設計変更): {successFolderPath_After_Item}");
                    Console.WriteLine($"エラーフォルダパス (設計変更): {errorFolderPath_After_Item}");
                    //
                    Console.WriteLine($"正常終了時コメント: {integrationStatusComment_Success}");
                    Console.WriteLine($"エラー終了時コメント: {integrationStatusComment_Error}");

                    #endregion
                    // *********************************************************************************
                    // * バリデーションチェック
                    // *********************************************************************************
                    // *********************************************************************************
                    // * 品目連携ファイルバリデーションチェック
                    // *********************************************************************************
                    #region 品目連携ファイルバリデーションチェック
                    var filesItem = Directory.GetFiles(targetFolderItem, "*.*", System.IO.SearchOption.TopDirectoryOnly)
                    .Where(f =>
                               Path.GetFileName(f).Contains(filePrefix_Item) &&
                               Path.GetFileName(f).Contains(strIraiNo));

                    if (filesItem.Count() > 1  ) {
                        // ファイルが存在しないもしくは複数ある場合の措置
                        // 別途エラー処理を入れる。
                        integrationErrorType_Item = true;
                    }
                    foreach (var file in filesItem)
                    {
                        // 存在するファイル分だけループ
                        Console.WriteLine(file);
                        bool isHeader = true;
                        long rowCount = 0;
                        foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                        {
                            rowCount++;
                            if (isHeader)
                            {
                                isHeader = false;
                            }
                            else
                            {
                                // ファイルの行単位でループ
                                // 1行を配列に分割
                                var columns = line.Split(',');

                                // バリデーション実行
                                var errors = CsvValidator.Validate<ItemRecord>(columns);

                                if (errors.Any())
                                {
                                    //
                                    Console.WriteLine($"品目連携ファイルの{rowCount}行目でエラーがあります：");
                                    errors.ForEach(e => Console.WriteLine("  - " + e));
                                    //
                                    afterIntegrationComment_Item += $"{rowCount}行目:";
                                    errors.ForEach(e => afterIntegrationComment_Item += $"  - {e}" + integrationStatusComment_Success);
                                    integrationErrorType_Item = true;
                                }
                                else
                                {
                                    //Console.WriteLine("OK: " + string.Join(", ", line));
                                    Console.WriteLine("OK");
                                }
                            }
                        }
                    }
                    #endregion
                    // *********************************************************************************
                    // * 品目連携ファイルバリデーションチェック終了
                    // *********************************************************************************
                    // *********************************************************************************
                    // * 品目構成連携ファイルバリデーションチェック
                    // *********************************************************************************
                    #region 品目構成連携ファイルバリデーションチェック
                    var filesItemStructure = Directory.GetFiles(targetFolderItemStructure, "*.*", System.IO.SearchOption.TopDirectoryOnly)
                    .Where(f =>
                               Path.GetFileName(f).Contains(filePrifix_ItemStructure) &&
                               Path.GetFileName(f).Contains(strIraiNo));

                    if (filesItemStructure.Count() > 1)
                    {
                        // ファイルが存在しないもしくは複数ある場合の措置
                        // 別途エラー処理を入れる。
                        integrationErrorType_ItemStructure = true;
                    }
                    foreach (var file in filesItemStructure)
                    {
                        // 存在するファイル分だけループ
                        Console.WriteLine(file);
                        bool isHeader = true;
                        long rowCount = 0;
                        foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                        {
                            rowCount++;
                            if (isHeader)
                            {
                                isHeader = false;
                            }
                            else
                            {
                                // ファイルの行単位でループ
                                // 1行を配列に分割
                                var columns = line.Split(',');

                                // バリデーション実行
                                var errors = CsvValidator.Validate<ItemStructureRecord>(columns);

                                if (errors.Any())
                                {
                                    //
                                    Console.WriteLine($"品目構成連携ファイルの{rowCount}行目でエラーがあります：");
                                    errors.ForEach(e => Console.WriteLine("  - " + e));
                                    //
                                    afterIntegrationComment_ItemStructure += $"{rowCount}行目:";
                                    errors.ForEach(e => afterIntegrationComment_ItemStructure += $"  - {e}" + integrationStatusComment_Success);
                                    integrationErrorType_ItemStructure = true;
                                }
                                else
                                {
                                    Console.WriteLine("OK");
                                }
                            }
                        }
                    }
                    #endregion
                    // *********************************************************************************
                    // * 品目構成連携ファイルバリデーションチェック終了
                    // *********************************************************************************
                    // *********************************************************************************
                    // * 設計変更連携ファイルバリデーションチェック
                    // *********************************************************************************
                    #region　設計変更連携ファイルバリデーションチェック
                    var filesDesignChange = Directory.GetFiles(targetFolderDesignChange, "*.*", System.IO.SearchOption.TopDirectoryOnly)
                    .Where(f =>
                               Path.GetFileName(f).Contains(filePrifix_DesignChange) &&
                               Path.GetFileName(f).Contains(strIraiNo));

                    if (filesDesignChange.Count() > 1)
                    {
                        // ファイルが存在しないもしくは複数ある場合の措置
                        // 別途エラー処理を入れる。
                        integrationErrorType_DesignChange = true;
                    }
                    foreach (var file in filesDesignChange)
                    {
                        // 存在するファイル分だけループ
                        Console.WriteLine(file);
                        bool isHeader = true;
                        long rowCount = 0;
                        //foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                        //foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                        //
                        using (var parser = new TextFieldParser(file, Encoding.GetEncoding("Shift_JIS")))
                        {
                            parser.TextFieldType = FieldType.Delimited;
                            parser.SetDelimiters(",");
                            parser.HasFieldsEnclosedInQuotes = true;

                            while (!parser.EndOfData)
                            {
                                string[] columns = parser.ReadFields();

                                // 改行除去（必要なら）
                                for (int i = 0; i < columns.Length; i++)
                                {
                                    columns[i] = columns[i]
                                        .Replace("\r", "")
                                        .Replace("\n", "");
                                }

                                rowCount++;
                                if (isHeader)
                                {
                                    isHeader = false;
                                }
                                else
                                {
                                    // 日付項目スラッシュを除く
                                    if (columns.Length > 20)
                                    {
                                        columns[20] = columns[20].Replace("/", "");
                                        columns[20] = columns[20].Replace(":", "");
                                        columns[20] = columns[20].Replace(" ", "");
                                    }
                                    if (columns.Length > 22)
                                    {
                                        columns[22] = columns[22].Replace("/", "");
                                        columns[22] = columns[22].Replace(":", "");
                                        columns[22] = columns[22].Replace(" ", "");
                                    }
                                    if (columns.Length > 24)
                                    {
                                        columns[24] = columns[24].Replace("/", "");
                                        columns[24] = columns[24].Replace(":", "");
                                        columns[24] = columns[24].Replace(" ", "");
                                    }

                                    columns[14] = columns[14].Replace("/", "");
                                    columns[14] = columns[14].Replace(":", "");
                                    columns[14] = columns[14].Replace(" ", "");

                                    // バリデーション実行
                                    var errors = CsvValidator.Validate<DesignChangeRecord>(columns);

                                    if (errors.Any())
                                    {
                                        //
                                        Console.WriteLine($"設計変更連携ファイルの{rowCount}行目でエラーがあります：");
                                        errors.ForEach(e => Console.WriteLine("  - " + e));
                                        //
                                        afterIntegrationComment_DesignChange += $"{rowCount}行目:";
                                        errors.ForEach(e => afterIntegrationComment_DesignChange += $"  - {e}" + integrationStatusComment_Success);
                                        integrationErrorType_DesignChange = true;
                                    }
                                    else
                                    {
                                        Console.WriteLine("OK");
                                    }
                                }
                            }
                        }
                    }
                    #endregion
                    // *********************************************************************************
                    // * 設計変更連携ファイルバリデーションチェック終了
                    // *********************************************************************************
                    // *********************************************************************************
                    // * バリデーションチェック終了
                    // *********************************************************************************
                    if (integrationErrorType_Item || integrationErrorType_ItemStructure || integrationErrorType_DesignChange)
                    {
                        Console.WriteLine("エラーが発生している為、データベースの更新は行われません");
                    }
                    else
                    {
                        // *********************************************************************************
                        // * データベ－ス更新
                        // *********************************************************************************
                        using (var transaction = connection.BeginTransaction())
                        {
                            try
                            {
                                // *********************************************************************************
                                // * 品目マスタ及び品目マスタ履歴の更新処理
                                // *********************************************************************************
                                #region 品目マスタ及び品目マスタ履歴の更新処理
                                foreach (var file in filesItem)
                                {
                                    // 存在するファイル分だけループ
                                    Console.WriteLine(file);
                                    bool isHeader = true;
                                    long rowCount = 0;
                                    foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                                    {
                                        rowCount++;
                                        if (isHeader)
                                        {
                                            isHeader = false;
                                        }
                                        else
                                        {
                                            // ファイルの行単位でループ
                                            // 1行を配列に分割
                                            var columns = line.Split(',');
                                            // 品目連携ファイルをオブジェクト「ItemRecord」に設定
                                            var itemRecord = new ItemRecord
                                            {
                                                ItemDocumentNumber = columns[0], // 図番/購入仕様書番号
                                                RevisionNumber = int.Parse(columns[1]), // 改訂番号
                                                ItemCode = columns[2], // 品目コード
                                                ItemName = columns[3], // 品目名
                                                ItemEnglishName = columns[4], // 品目英名
                                                UnusableFlag = columns[5], // 使用不可区分
                                                NewItemNumber = columns.Length > 6 ? columns[6] : null, // 変更後品番 (Nullable)
                                                ClassName = columns.Length > 7 ? columns[7] : null, // クラス名 (Nullable)
                                                ItemLinkageFlag = columns.Length > 8 ? columns[8] : null, // 品番連携区分 (Nullable)
                                                AssemblyFlag = columns.Length > 9 ? columns[9] : null, // 組立区分 (Nullable)
                                                VarietyCode = columns.Length > 10 ? columns[10] : null, // 品種コード (Nullable)
                                                MaterialCode = columns.Length > 11 ? columns[11] : null, // 材質コード (Nullable)
                                                Remarks = columns.Length > 12 ? columns[12] : null // 備考 (Nullable)
                                            };
                                            // 品目マスタのエンティティクラスを実体化
                                            var itemMasterRepository = new ItemMasterRepository(connection, transaction);
                                            // 品目マスタのデータ取得
                                            string itemCode = itemRecord.ItemCode;  // 品目コードを取得キー設定
                                            ItemMaster item = itemMasterRepository.GetItemById(itemCode);
                                            if (item != null)
                                            {
                                                // 品目マスタが存在した場合更新処理を行う
                                                Console.WriteLine($"品目コード:[itemRecord.ItemCode]を更新します");
                                                //
                                                item.FGDELE = Convert.ToDecimal(0);                                 // 削除フラグ
                                                item.SPUUSR = registrationUserID;                                   // 更新担当者コード
                                                item.SPUDTM = Convert.ToDecimal(integrationDateTime);
                                                // 更新日時
                                                item.SPUPGM = registrationProgramId;                                // 更新プログラムID
                                                //item.CDHINM = itemRecord.ItemCode;                                // *** 品目コードは更新キーなのでセット不要
                                                item.NMHINM = GetTrimmedString(itemRecord.ItemName, 66);             // 品目名 
                                                item.ENHINM = GetTrimmedString(itemRecord.ItemEnglishName, 66);      // 品目英名
                                                item.CDHINS = itemRecord.VarietyCode;                               // 品種コード
                                                item.NMHISJ = GetTrimmedString(itemRecord.ItemName, 66);            // 品種名に品目名をセット
                                                item.NMHISE = GetTrimmedString(itemRecord.ItemEnglishName, 66);     // 品種英名に品目英名をセット
                                                item.KBKMTT = itemRecord.AssemblyFlag;                              // 組立区分
                                                item.NOREVS = itemRecord.RevisionNumber;                            // 改訂番号
                                                switch (itemRecord.ItemLinkageFlag)                                 // 使用不可区分
                                                {
                                                    case "3":
                                                        item.KBSING = "1";
                                                        break;
                                                    case "4":
                                                        item.KBSING = "0";
                                                        break;
                                                    default:
                                                        item.KBSING = itemRecord.UnusableFlag;
                                                        break;
                                                }
                                                item.VLZUBN = itemRecord.ItemDocumentNumber;                        // 図番/購入仕様書番号
                                                item.NMBCLS = itemRecord.ClassName;                                 // Base-Rightクラス名
                                                item.CDHHIN = itemRecord.NewItemNumber;                             // 変更後品目コード
                                                item.TXBRTX = itemRecord.Remarks;                                   // BR特記事項
                                                item.KBHREN = itemRecord.ItemLinkageFlag;                           // 品番連携区分
                                                item.CDZAIS1 = itemRecord.MaterialCode;                             // 材質コード1
                                                itemMasterRepository.UpdateItem(item);

                                                // 他のフィールドを表示
                                            }
                                            else
                                            {
                                                // 品目マスタが存在しない場合追加処理を行う
                                                Console.WriteLine($"品目コード:[itemRecord.ItemCode]を追加します");
                                                //
                                                ItemMaster item2 = new ItemMaster();
                                                item2.FGDELE = Convert.ToDecimal(0);                                // 削除フラグ
                                                item2.SPIUSR = registrationUserID;                                  // 登録担当者コード
                                                item2.SPIDTM = Convert.ToDecimal(integrationDateTime);
                                                // 登録日時
                                                item2.SPIPGM = registrationProgramId;                               // 登録プログラムID
                                                item2.SPUUSR = registrationUserID;                                  // 更新担当者コード
                                                item2.SPUDTM = Convert.ToDecimal(integrationDateTime);
                                                // 更新日時
                                                item2.SPUPGM = registrationProgramId;                               // 更新プログラムID
                                                //
                                                item2.CDHINM = itemRecord.ItemCode;                                 // 品目コード
                                                item2.NMHINM = GetTrimmedString(itemRecord.ItemName, 66);            // 品目名 
                                                item2.ENHINM = GetTrimmedString(itemRecord.ItemEnglishName, 66);     // 品目英名
                                                item2.CDHINS = itemRecord.VarietyCode;                               // 品種コード
                                                item2.NMHISJ = GetTrimmedString(itemRecord.ItemName, 66);            // 品種名に品目名をセット
                                                item2.NMHISE = GetTrimmedString(itemRecord.ItemEnglishName, 66);     // 品種英名に品目英名をセット
                                                //item2.NMHISJ = string.Empty;
                                                //item2.NMHISE = string.Empty;
                                                item2.KBKMTT = itemRecord.AssemblyFlag;                             // 組立区分
                                                item2.NOREVS = itemRecord.RevisionNumber;                           // 改訂番号
                                                switch (itemRecord.ItemLinkageFlag)                                 // 使用不可区分
                                                {
                                                    case "3":
                                                        item2.KBSING = "1";
                                                        break;
                                                    case "4":
                                                        item2.KBSING = "0";
                                                        break;
                                                    default:
                                                        item2.KBSING = itemRecord.UnusableFlag;
                                                        break;
                                                }
                                                item2.VLZUBN = itemRecord.ItemDocumentNumber;                       // 図番/購入仕様書番号
                                                item2.NMBCLS = itemRecord.ClassName;                                // Base-Rightクラス名
                                                item2.CDHHIN = itemRecord.NewItemNumber;                            // 変更後品目コード
                                                item2.TXBRTX = itemRecord.Remarks;                                  // BR特記事項
                                                item2.KBHREN = itemRecord.ItemLinkageFlag;                          // 品番連携区分
                                                item2.CDZAIS1 = itemRecord.MaterialCode;                            // 材質コード1
                                                item2.KBTEHI = "999";                                               // 手配区分
                                                itemMasterRepository.InsertItem(item2);
                                                // 他のフィールドを表示
                                            }
                                            //
                                            ItemMasterHistoryRepository ItemMasterHistoryRepository = new ItemMasterHistoryRepository(connection, transaction);
                                            ItemMasterHistory ItemMasterHistory = new ItemMasterHistory();
                                            ItemMasterHistory.FGDELE = Convert.ToDecimal(0);                                                // 削除フラグ
                                            ItemMasterHistory.SPIUSR = registrationUserID;                                                  // 登録担当者コード
                                            ItemMasterHistory.SPIDTM = Convert.ToDecimal(integrationDateTime);                              // 登録日時
                                            ItemMasterHistory.SPIPGM = registrationProgramId;                                               // 登録プログラムID
                                            ItemMasterHistory.SPUUSR = registrationUserID;                                                  // 更新担当者コード
                                            ItemMasterHistory.SPUDTM = Convert.ToDecimal(integrationDateTime);                              // 更新日時
                                            ItemMasterHistory.SPUPGM = registrationProgramId;                                               // 更新プログラムID
                                            ItemMasterHistory.HistoryDate = Convert.ToInt64(DateTime.Now.ToString("yyyyMMdd"));
                                            ItemMasterHistory.HistoryTime = Convert.ToInt64(DateTime.Now.ToString("HHmmss"));
                                            ItemMasterHistory.ItemCode = itemRecord.ItemCode;                                 // 品目コード
                                            ItemMasterHistory.ItemName = GetTrimmedString(itemRecord.ItemName, 66);            // 品目名 
                                            ItemMasterHistory.ItemEnglishName = GetTrimmedString(itemRecord.ItemEnglishName, 66);     // 品目英名
                                            ItemMasterHistory.VarietyCode = itemRecord.VarietyCode;                               // 品種コード
                                            ItemMasterHistory.VarietyNameJapanese = GetTrimmedString(itemRecord.ItemName, 66);            // 品種名に品目名をセット
                                            ItemMasterHistory.VarietyNameEnglish = GetTrimmedString(itemRecord.ItemEnglishName, 66);     // 品種英名に品目英名をセット
                                            ItemMasterHistory.AssemblyCode = itemRecord.AssemblyFlag;                             // 組立区分
                                            ItemMasterHistory.RevisionNumber = itemRecord.RevisionNumber;                           // 改訂番号
                                            switch (itemRecord.ItemLinkageFlag)                                 // 使用不可区分
                                            {
                                                case "3":
                                                    ItemMasterHistory.UnavailableFlag = "1";
                                                    break;
                                                case "4":
                                                    ItemMasterHistory.UnavailableFlag = "0";
                                                    break;
                                                default:
                                                    ItemMasterHistory.UnavailableFlag = itemRecord.UnusableFlag;
                                                    break;
                                            }
                                            ItemMasterHistory.DrawingNumber = itemRecord.ItemDocumentNumber;                       // 図番/購入仕様書番号
                                            ItemMasterHistory.BaseRightClassName = itemRecord.ClassName;                                // Base-Rightクラス名
                                            ItemMasterHistory.BRRemarks = itemRecord.Remarks;                                  // BR特記事項
                                            ItemMasterHistory.ItemNumberLinkFlag = itemRecord.ItemLinkageFlag;                          // 品番連携区分
                                            ItemMasterHistory.MaterialCode1 = itemRecord.MaterialCode;                            // 材質コード1
                                            //
                                            ItemMasterHistoryRepository.Insert(connection, ItemMasterHistory);
                                        }
                                    }
                                }
                                #endregion
                                // *********************************************************************************
                                // * 品目構成マスタの更新処理
                                // *********************************************************************************
                                #region 品目構成マスタの更新処理
                                foreach (var file in filesItemStructure)
                                {
                                    // 存在するファイル分だけループ
                                    Console.WriteLine(file);
                                    bool isHeader = true;
                                    long rowCount = 0;
                                    foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                                    {
                                        // 最初に該当品目構成データの削除処理を実施する。
                                        rowCount++;
                                        if (isHeader)
                                        {
                                            isHeader = false;
                                        }
                                        else
                                        {
                                            // ファイルの行単位でループ
                                            // 1行を配列に分割
                                            var columns = line.Split(',');
                                            // 品目構成連携ファイルをオブジェクト「ItemStructureRecord」に設定
                                            var itemStructureRecord = new ItemStructureRecord
                                            {
                                                parentItemNumber = columns[0],                      //親品目コード
                                                balloonNumber = !int.TryParse(columns[1] , out int result)
                                                ? 0
                                                : int.Parse(columns[1]), //風船番号
                                                ItemStructureLineNo = decimal.Parse(columns[2]),    //品目構成行No
                                                childItemNumber = columns[3],                       //子品目コード
                                                quantity = decimal.Parse(columns[4]),               //員数
                                            };
                                            // 品目構成マスタのエンティティクラスを実体化
                                            var ItemMasterStructureRepository = new ItemMasterStructureRepository(connection, transaction);
                                            // 品目構成マスタのデータ取得
                                            ItemMasterStructure itemStructure = ItemMasterStructureRepository.GetItemStructureById(itemStructureRecord.parentItemNumber, itemStructureRecord.ItemStructureLineNo);
                                            if (itemStructure != null)
                                            {
                                                // 品目構成マスタが存在した場合構成削除処理を行う
                                                Console.WriteLine($"品目構成:[itemStructureRecord.parentItemNumber]を削除します");
                                                //
                                                ItemMasterStructureRepository.DeleteItemStructure(itemStructure);
                                                // 他のフィールドを表示
                                            }
                                        }
                                    }
                                    isHeader = true;
                                    rowCount = 0;
                                    foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                                    {
                                        // 次に該当品目構成の追加を行う。
                                        rowCount++;
                                        if (isHeader)
                                        {
                                            isHeader = false;
                                        }
                                        else
                                        {
                                            // ファイルの行単位でループ
                                            // 1行を配列に分割
                                            var columns = line.Split(',');
                                            // 品目構成連携ファイルをオブジェクト「ItemStructureRecord」に設定
                                            var itemStructureRecord = new ItemStructureRecord
                                            {
                                                parentItemNumber = columns[0],                      //親品目コード
                                                balloonNumber = !int.TryParse(columns[1], out int result)
                                                ? 0
                                                : int.Parse(columns[1]), //風船番号
                                                ItemStructureLineNo = decimal.Parse(columns[2]),    //品目構成行No
                                                childItemNumber = columns[3],                       //子品目コード
                                                quantity = decimal.Parse(columns[4]),               //員数
                                            };
                                            // 品目構成マスタのエンティティクラスを実体化
                                            var ItemMasterStructureRepository = new ItemMasterStructureRepository(connection, transaction);
                                            // 品目構成マスタ追加処理
                                            Console.WriteLine($"品目構成:[itemStructureRecord.parentItemNumber] - [itemStructureRecord.ItemStructureLineNo]を追加します");
                                            //
                                            ItemMasterStructure itemStructure2 = new ItemMasterStructure();
                                            itemStructure2.DeleteFlag = 0;                                                  // 削除フラグ
                                            itemStructure2.RegisteredUserCode = registrationUserID;                         // 登録担当者コード
                                            itemStructure2.RegisteredDateTime = Convert.ToDecimal(integrationDateTime);
                                                                                                                            // 登録日時
                                            itemStructure2.RegisteredProgramId = registrationProgramId;                     // 登録プログラムID
                                            itemStructure2.UpdatedUserCode = registrationUserID;                            // 更新担当者コード
                                            itemStructure2.UpdatedDateTime = Convert.ToDecimal(integrationDateTime);        // 更新日時
                                            itemStructure2.UpdatedProgramId = registrationProgramId;                        // 更新プログラムID
                                            itemStructure2.ParentItemCode = itemStructureRecord.parentItemNumber;           // 親品目コード
                                            itemStructure2.ItemStructureLineNo = itemStructureRecord.ItemStructureLineNo;   // 品目構成行No
                                            itemStructure2.DisplayLineNo = itemStructureRecord.ItemStructureLineNo;         // 表示行No
                                            itemStructure2.ChildItemCode = itemStructureRecord.childItemNumber;             // 子品目コード
                                            itemStructure2.BalloonNumber = itemStructureRecord.balloonNumber;               // 風船番号
                                            itemStructure2.Quantity = itemStructureRecord.quantity;                         // 員数
                                            
                                            ItemMasterStructureRepository.InsertItemStructure(itemStructure2);
                                        }
                                    }
                                }
                                #endregion
                                // *********************************************************************************
                                // * 設計変更マスタの更新処理
                                // *********************************************************************************
                                #region 設計変更マスタの更新処理
                                foreach (var file in filesDesignChange)
                                {
                                    // 存在するファイル分だけループ
                                    Console.WriteLine(file);
                                    bool isHeader = true;
                                    long rowCount = 0;
                                    using (var parser = new TextFieldParser(file, Encoding.GetEncoding("Shift_JIS")))
                                    {
                                        parser.TextFieldType = FieldType.Delimited;
                                        parser.SetDelimiters(",");
                                        parser.HasFieldsEnclosedInQuotes = true;

                                        while (!parser.EndOfData)
                                        {
                                            string[] columns = parser.ReadFields();

                                            //// 改行除去（必要なら）
                                            //for (int i = 0; i < columns.Length; i++)
                                            //{
                                            //    columns[i] = columns[i]
                                            //        .Replace("\r", "")
                                            //        .Replace("\n", "");
                                            //}


                                            // 最初に該当設計変更データの削除処理を実施する。
                                            rowCount++;
                                            if (isHeader)
                                            {
                                                isHeader = false;
                                            }
                                            else
                                            {


                                                // 日付項目スラッシュを除く
                                                if (columns.Length > 20)
                                                {
                                                    columns[20] = columns[20].Replace("/", "");
                                                    columns[20] = columns[20].Replace(":", "");
                                                    columns[20] = columns[20].Replace(" ", "");
                                                }
                                                if (columns.Length > 22)
                                                {
                                                    columns[22] = columns[22].Replace("/", "");
                                                    columns[22] = columns[22].Replace(":", "");
                                                    columns[22] = columns[22].Replace(" ", "");
                                                }
                                                if (columns.Length > 24)
                                                {
                                                    columns[24] = columns[24].Replace("/", "");
                                                    columns[24] = columns[24].Replace(":", "");
                                                    columns[24] = columns[24].Replace(" ", "");
                                                }
                                                // 設計変更連携ファイルをオブジェクト「DesignChangeRecord」に設定
                                                var DesignChangeRecord = new DesignChangeRecord
                                                {
                                                    Creator = columns[19],                          //	作成者
                                                    NotificationNumber = columns[0],                //	設計変更通知書番号
                                                    Classification = columns[1],                    //	区分
                                                    CodeNumber = columns[2],                        //	コード番号
                                                    Name = columns[3],                              //	名称
                                                    Model = columns[4],                             //	機種
                                                    NewItemNumber = columns[5],                     //	新品目番号
                                                    OldItemNumber = columns[6],                     //	旧品目番号
                                                    NewDrawingNumber = columns[7],                  //	新図面番号
                                                    OldDrawingNumber = columns[8],                  //	旧図面番号
                                                    Reason = columns[9],                            //	理由
                                                    RevisionContactNo = columns[10],                //	改定連絡所No.
                                                    FeedbackSheetNo = columns[11],                  //	フィードバックシートNo.
                                                    Item = columns[12],                             //	項目
                                                    BOMChange = columns[13],                        //	BOM構成変更
                                                    ProcessingChange = columns[14],                 //	加工変更(MC含む)
                                                    SpecificationReview = columns[15],              //	認定仕様範囲の再確認
                                                    DocumentReplacement = columns[16],              //	設計図書差し替え
                                                    StockHandling = columns[17],                    //	仕掛品(H,B,S,A,KK番)・在庫品の処理
                                                    ChangeReasonAndContent = columns[18],           //	変更理由及び変更内容
                                                    CreationDate = Convert.ToInt64(columns[20]),    //	作成日
                                                    Checker = columns[21],                          //	検図者
                                                    CheckDate = Convert.ToInt64(columns[22]),       //	検図日
                                                    Approver = columns[23],                         //	承認者
                                                    ApprovalDate = Convert.ToInt64(columns[24]),    //	承認日
                                                };
                                                // 設計変更マスタのエンティティクラスを実体化
                                                var DesignChangeFileRepository = new DesignChangeFileRepository(connection, transaction);
                                                // 設計変更マスタのデータ取得
                                                DesignChangeFile DesignChange = DesignChangeFileRepository.GetDesignChangeFileById(DesignChangeRecord.NotificationNumber);
                                                if (DesignChange != null)
                                                {
                                                    // 設計変更マスタが存在した場合更新処理を行う
                                                    Console.WriteLine($"設計変更:[DesignChangeRecord.NotificationNumber]を更新します");
                                                    DesignChange.UpdatedUserCode = registrationUserID;                                 // 更新担当者コード
                                                    DesignChange.UpdatedDateTime = Convert.ToDecimal(integrationDateTime);             // 更新日時
                                                    DesignChange.UpdatedProgramId = registrationProgramId;                             // 更新プログラムID
                                                    //
                                                    DesignChange.NotificationNumber = DesignChangeRecord.NotificationNumber;           // 設計変更通知番号
                                                    DesignChange.NotificationCategory = DesignChangeRecord.Classification;             // 設計変更区分

                                                    // コード番号、枝番はBaseRgihtでは自由記述欄であり連携対象から外す
                                                    //DesignChange2.CodeNumber = (DesignChangeRecord.CodeNumber).Length > 10
                                                    //    ? (DesignChangeRecord.CodeNumber).Substring(0, 10)
                                                    //    : DesignChangeRecord.CodeNumber;                                              // コード番号
                                                    ////
                                                    //DesignChange2.BranchNumber = (DesignChangeRecord.CodeNumber).Length > 10
                                                    //    ? DesignChangeRecord.CodeNumber.Substring(11, DesignChangeRecord.CodeNumber.Length)
                                                    //    : String.Empty;                                                               // 枝番
                                                    //
                                                    DesignChange.Name = DesignChangeRecord.Name;                                       // 名称
                                                    DesignChange.ModelCode = DesignChangeRecord.Model;                                 // 型式コード
                                                    DesignChange.NewItemCode = DesignChangeRecord.NewItemNumber;                       // 新品目コード
                                                    DesignChange.OldItemCode = DesignChangeRecord.OldItemNumber;                       // 旧品目コード
                                                    DesignChange.NewDrawingNumber = DesignChangeRecord.NewDrawingNumber;               // 新図面番号
                                                    DesignChange.OldDrawingNumber = DesignChangeRecord.OldDrawingNumber;               // 旧図面番号
                                                    DesignChange.Reason = DesignChangeRecord.Reason;                                   // 理由
                                                    DesignChange.RevisionDocumentNumber = DesignChangeRecord.RevisionContactNo;        // 改訂連絡書No
                                                    DesignChange.FeedbackSheetNumber = DesignChangeRecord.FeedbackSheetNo;             // フィードバックシートNo
                                                    DesignChange.Item = DesignChangeRecord.Item;                                       // 項目
                                                    DesignChange.BOMChangeFlag = String.Empty;                                         // BOM構成変更フラグ
                                                    DesignChange.ProcessingChangeFlag = String.Empty;                                  // 加工変更フラグ
                                                    DesignChange.SpecificationReviewFlag = String.Empty;                               // 認定仕様範囲の再確認
                                                    DesignChange.DocumentReplacementFlag = String.Empty;                               // 設計図書差し替え

                                                    // 以下4項目は、BaseRight側は日本語文字列、B-Core側はフラグであり、そのままセットできない為、連携対象から外す
                                                    //DesignChange2.BOMChangeFlag = DesignChangeRecord.BOMChange;                       // BOM構成変更フラグ
                                                    //DesignChange2.ProcessingChangeFlag = DesignChangeRecord.ProcessingChange;         // 加工変更フラグ
                                                    //DesignChange2.SpecificationReviewFlag = DesignChangeRecord.SpecificationReview;   // 認定仕様範囲の再確認
                                                    //DesignChange2.DocumentReplacementFlag = DesignChangeRecord.DocumentReplacement;   // 設計図書差し替え

                                                    DesignChange.StockHandling = DesignChangeRecord.StockHandling;                     // 仕掛品・在庫品の処理
                                                    DesignChange.ChangeReasonContent = DesignChangeRecord.ChangeReasonAndContent;      // 変更理由・内容
                                                    DesignChange.Creator = DesignChangeRecord.Creator;                                 // 作成者
                                                    DesignChange.CreationDate = DesignChangeRecord.CreationDate;                       // 作成日時
                                                    DesignChange.Checker = DesignChangeRecord.Checker;                                 // 検図者
                                                    DesignChange.CheckDate = DesignChangeRecord.CheckDate;                             // 検図日
                                                    DesignChange.Approver = DesignChangeRecord.Approver;                               // 承認者
                                                    DesignChange.ApprovalDate = DesignChangeRecord.ApprovalDate;                       // 承認日
                                                    //
                                                    DesignChangeFileRepository.UpdateDesignChangeFile(DesignChange);
                                                    // 他のフィールドを表示
                                                }
                                                else
                                                {
                                                    // 品目マスタが存在しない場合追加処理を行う
                                                    Console.WriteLine($"設計変更:[DesignChangeRecord.NotificationNumber]を追加します");
                                                    //
                                                    DesignChangeFile DesignChange2 = new DesignChangeFile();
                                                    DesignChange2.DeleteFlag = 0;                                                       // 削除フラグ
                                                    DesignChange2.RegisteredUserCode = registrationUserID;                              // 登録担当者コード
                                                    DesignChange2.RegisteredDateTime = Convert.ToDecimal(integrationDateTime);          // 登録日時
                                                    DesignChange2.RegisteredProgramId = registrationProgramId;                          // 登録プログラムID
                                                    DesignChange2.UpdatedUserCode = registrationUserID;                                 // 更新担当者コード
                                                    DesignChange2.UpdatedDateTime = Convert.ToDecimal(integrationDateTime);             // 更新日時
                                                    DesignChange2.UpdatedProgramId = registrationProgramId;                             // 更新プログラムID
                                                    //
                                                    DesignChange2.NotificationNumber = DesignChangeRecord.NotificationNumber;           // 設計変更通知番号
                                                    DesignChange2.NotificationCategory = DesignChangeRecord.Classification;             // 設計変更区分


                                                    // コード番号、枝番はBaseRgihtでは自由記述欄であり連携対象から外してNullをセットする
                                                    //DesignChange2.CodeNumber = (DesignChangeRecord.CodeNumber).Length > 10
                                                    //    ? (DesignChangeRecord.CodeNumber).Substring(0, 10)
                                                    //    : DesignChangeRecord.CodeNumber;                                              // コード番号
                                                    ////
                                                    //DesignChange2.BranchNumber = (DesignChangeRecord.CodeNumber).Length > 10
                                                    //    ? DesignChangeRecord.CodeNumber.Substring(11, DesignChangeRecord.CodeNumber.Length)
                                                    //    : String.Empty;                                                               // 枝番
                                                    //
                                                    DesignChange2.CodeNumber = string.Empty;
                                                    DesignChange2.BranchNumber = string.Empty;
                                                    //

                                                    DesignChange2.Name = DesignChangeRecord.Name;                                       // 名称
                                                    DesignChange2.ModelCode = DesignChangeRecord.Model;                                 // 型式コード
                                                    DesignChange2.NewItemCode = DesignChangeRecord.NewItemNumber;                       // 新品目コード
                                                    DesignChange2.OldItemCode = DesignChangeRecord.OldItemNumber;                       // 旧品目コード
                                                    DesignChange2.NewDrawingNumber = DesignChangeRecord.NewDrawingNumber;               // 新図面番号
                                                    DesignChange2.OldDrawingNumber = DesignChangeRecord.OldDrawingNumber;               // 旧図面番号
                                                    DesignChange2.Reason = DesignChangeRecord.Reason;                                   // 理由
                                                    DesignChange2.RevisionDocumentNumber = DesignChangeRecord.RevisionContactNo;        // 改訂連絡書No
                                                    DesignChange2.FeedbackSheetNumber = DesignChangeRecord.FeedbackSheetNo;             // フィードバックシートNo
                                                    DesignChange2.Item = DesignChangeRecord.Item;                                       // 項目

                                                    // 以下4項目は、BaseRight側は日本語文字列、B-Core側はフラグであり、そのままセットできない為、連携対象から外し、Nullをセットする
                                                    //DesignChange2.BOMChangeFlag = DesignChangeRecord.BOMChange;                       // BOM構成変更フラグ
                                                    //DesignChange2.ProcessingChangeFlag = DesignChangeRecord.ProcessingChange;         // 加工変更フラグ
                                                    //DesignChange2.SpecificationReviewFlag = DesignChangeRecord.SpecificationReview;   // 認定仕様範囲の再確認
                                                    //DesignChange2.DocumentReplacementFlag = DesignChangeRecord.DocumentReplacement;   // 設計図書差し替え
                                                    DesignChange2.BOMChangeFlag = String.Empty;                                         // BOM構成変更フラグ
                                                    DesignChange2.ProcessingChangeFlag = String.Empty;                                  // 加工変更フラグ
                                                    DesignChange2.SpecificationReviewFlag = String.Empty;                               // 認定仕様範囲の再確認
                                                    DesignChange2.DocumentReplacementFlag = String.Empty;                               // 設計図書差し替え
                                                    //


                                                    DesignChange2.StockHandling = DesignChangeRecord.StockHandling;                     // 仕掛品・在庫品の処理
                                                    DesignChange2.ChangeReasonContent = DesignChangeRecord.ChangeReasonAndContent;      // 変更理由・内容
                                                    DesignChange2.Creator = DesignChangeRecord.Creator;                                 // 作成者
                                                    DesignChange2.CreationDate = DesignChangeRecord.CreationDate;                       // 作成日時
                                                    DesignChange2.Checker = DesignChangeRecord.Checker;                                 // 検図者
                                                    DesignChange2.CheckDate = DesignChangeRecord.CheckDate;                             // 検図日
                                                    DesignChange2.Approver = DesignChangeRecord.Approver;                               // 承認者
                                                    DesignChange2.ApprovalDate = DesignChangeRecord.ApprovalDate;                       // 承認日
                                                    //
                                                    DesignChangeFileRepository.InsertDesignChangeFile(DesignChange2);
                                                }
                                            }
                                        }
                                    }
                                }
                                #endregion

                                transaction.Commit();
                            }
                            catch (Exception ex)
                            {
                                // エラーが発生した場合ロールバック
                                transaction.Rollback();
                                // エラー状態を設定
                            }
                            // *********************************************************************************
                            // * コミット実施（品目・品目構成・設計変更が全てOKのときのみ
                            // *********************************************************************************
                        }
                        // *********************************************************************************
                        // * データベ－ス更新終了
                        // *********************************************************************************
                    }
                    // *********************************************************************************
                    // * バリデーションチェック正常時の処理終了
                    // *********************************************************************************
                    // *********************************************************************************
                    // * BaseRight連携履歴追加処理
                    // *********************************************************************************
                    if (integrationErrorType_Item || integrationErrorType_ItemStructure || integrationErrorType_DesignChange)
                    {
                        IntegrationErrorSatus = "9"; // エラー
                    }
                    else
                    {
                        IntegrationErrorSatus = "1"; //正常
                    }
                    // *********************************************************************************
                    // * BaseRight連携履歴（品目）
                    // *********************************************************************************
                    #region BaseRight連携履歴（品目）
                    foreach (var file in filesItem)
                    {
                        // 存在するファイル分だけループ
                        Console.WriteLine(file);
                        var BRNo = IdNumbering.UpdateCMSABNV99(registrationUserID, integrationDateTime, registrationProgramId, connection);
                        BaseRightHistoryHeader header = new BaseRightHistoryHeader
                        {
                            DeleteFlag = 0,                                                 // 削除フラグ
                            RegisteredUserCode = registrationUserID,                        // 登録担当者コード
                            RegisteredDateTime = Convert.ToInt64(integrationDateTime),      // 登録日時
                            RegisteredProgramID = registrationProgramId,                    // 登録プログラムID
                            UpdatedUserCode = registrationUserID,                           // 更新担当者コード
                            UpdatedDateTime = Convert.ToInt64(integrationDateTime),         // 更新日時
                            UpdatedProgramID = registrationProgramId,                       // 更新プログラムID
                            BaseRightNo = BRNo.ToString("D10"),                             // BR連携No
                            BaseRightCategory = "1",                                        // 1:品目
                            LinkedDate = Convert.ToInt64(integrationDateTime),
                            Sequence = 1,
                            Status = integrationErrorType_Item is true
                                        ? "9"
                                        : "1",
                            Comment = afterIntegrationComment_Item != null
                                      ? GetTrimmedString(afterIntegrationComment_Item, Math.Min(afterIntegrationComment_Item.Length, 256))
                                      : string.Empty,
                            SourceFilePath = file != null
                                      ? GetTrimmedString(file, Math.Min((file).Length, 256))
                                      : string.Empty,
                            DestinationFilePath = integrationErrorType_Item is true
                                      ? GetTrimmedString(errorTargetFolderItem, Math.Min(errorTargetFolderItem.Length, 256))
                                      : GetTrimmedString(successTargetFolderItem, Math.Min(successTargetFolderItem.Length, 256))
                        };
                        bool isHeader = true;
                        long rowCount = 0;
                        foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                        {
                            rowCount++;
                            if (isHeader)
                            {
                                isHeader = false;
                            }
                            else
                            {
                                // ファイルの行単位でループ
                                // 1行を配列に分割
                                var columns = line.Split(',');
                                header.Details.Add(new BaseRightHistoryDetailItem
                                {
                                    DeleteFlag = 0,                                                 // 削除フラグ
                                    RegisteredUserCode = registrationUserID,                        // 登録担当者コード
                                    RegisteredDateTime = Convert.ToInt64(integrationDateTime),      // 登録日時
                                    RegisteredProgramID = registrationProgramId,                    // 登録プログラムID
                                    UpdatedUserCode = registrationUserID,                           // 更新担当者コード
                                    UpdatedDateTime = Convert.ToInt64(integrationDateTime),         // 更新日時
                                    UpdatedProgramID = registrationProgramId,                       // 更新プログラムID
                                    BaseRightNo = BRNo.ToString("D10"),                             // BR連携No
                                    ItemLineNo = Convert.ToInt32(rowCount),                         // BR連携品目行No
                                    DesignNo = columns.Length > 0 && columns[0] != null
                                      ? columns[0].Substring(0, Math.Min(columns[0].Length, 512))
                                      : string.Empty,                                               // 図番/購入仕様書番号
                                    ChangeSymbol = columns.Length > 1 && columns[1] != null
                                      ? columns[1].Substring(0, Math.Min(columns[1].Length, 512))
                                      : string.Empty,                                               // 改訂番号
                                    ItemNo = columns.Length > 2 && columns[2] != null
                                      ? columns[2].Substring(0, Math.Min(columns[2].Length, 512))
                                      : string.Empty,                                               // 品目コード
                                    StandardName_JP = columns.Length > 3 && columns[3] != null
                                      ? columns[3].Substring(0, Math.Min(columns[3].Length, 512))
                                      : string.Empty,                                               // 品目名
                                    StandardName_EN = columns.Length > 4 && columns[4] != null
                                      ? columns[4].Substring(0, Math.Min(columns[4].Length, 512))
                                      : string.Empty,                                               // 品目英名
                                    UsageForbiddenCategory = columns.Length > 5 && columns[5] != null
                                      ? columns[5].Substring(0, Math.Min(columns[5].Length, 512))
                                      : string.Empty,                                               // 使用不可区分
                                    ChangedItemNo = columns.Length > 6 && columns[6] != null
                                      ? columns[6].Substring(0, Math.Min(columns[6].Length, 512))
                                      : string.Empty,                                               // 変更後品番
                                    ClassName = columns.Length > 7 && columns[7] != null
                                      ? columns[7].Substring(0, Math.Min(columns[7].Length, 512))
                                      : string.Empty,                                               // クラス名
                                    ItemLinkCategory = columns.Length > 8 && columns[8] != null
                                      ? columns[8].Substring(0, Math.Min(columns[8].Length, 512))
                                      : string.Empty,                                               // 品番連携区分
                                    AssemblyCategory = columns.Length > 9 && columns[9] != null
                                      ? columns[9].Substring(0, Math.Min(columns[9].Length, 512))
                                      : string.Empty,                                               // 組立区分
                                    SpeciesCode = columns.Length > 10 && columns[10] != null
                                      ? columns[10].Substring(0, Math.Min(columns[10].Length, 512))
                                      : string.Empty,                                               // 品種コード
                                    MaterialCode = columns.Length > 11 && columns[11] != null
                                      ? columns[11].Substring(0, Math.Min(columns[11].Length, 512))
                                      : string.Empty,                                               // 材質コード
                                    Remarks = columns.Length > 12 && columns[12] != null
                                      ? columns[12].Substring(0, Math.Min(columns[12].Length, 512))
                                      : string.Empty                                                // 備考
                                });
                            }
                        }
                        //
                        header.Insert(connection);
                    }
                    #endregion

                    // *********************************************************************************
                    // * BaseRight連携履歴（品目構成）
                    // *********************************************************************************
                    #region BaseRight連携履歴（品目構成）
                    foreach (var file in filesItemStructure)
                    {
                        // 存在するファイル分だけループ
                        Console.WriteLine(file);
                        var BRNo = IdNumbering.UpdateCMSABNV99(registrationUserID, integrationDateTime, registrationProgramId, connection);
                        BaseRightHistoryHeader header = new BaseRightHistoryHeader
                        {
                            DeleteFlag = 0,                                                 // 削除フラグ
                            RegisteredUserCode = registrationUserID,                        // 登録担当者コード
                            RegisteredDateTime = Convert.ToInt64(integrationDateTime),      // 登録日時
                            RegisteredProgramID = registrationProgramId,                    // 登録プログラムID
                            UpdatedUserCode = registrationUserID,                           // 更新担当者コード
                            UpdatedDateTime = Convert.ToInt64(integrationDateTime),         // 更新日時
                            UpdatedProgramID = registrationProgramId,                       // 更新プログラムID
                            BaseRightNo = BRNo.ToString("D10"),                             // BR連携No
                            BaseRightCategory = "2",                                        // 2:品目構成
                            LinkedDate = Convert.ToInt64(integrationDateTime),
                            Sequence = 1,
                            Status = integrationErrorType_ItemStructure is true
                                        ? "9"
                                        : "1",
                            Comment = afterIntegrationComment_ItemStructure != null
                                      ? GetTrimmedString(afterIntegrationComment_ItemStructure, Math.Min(afterIntegrationComment_ItemStructure.Length, 256))
                                      : string.Empty,
                            SourceFilePath = file != null
                                        ? GetTrimmedString(file, Math.Min((file).Length, 256))
                                      : string.Empty,
                            DestinationFilePath = integrationErrorType_ItemStructure is true
                                      ? GetTrimmedString(errorTargetFolderItemStructure, Math.Min(errorTargetFolderItemStructure.Length, 256))
                                      : GetTrimmedString(successTargetFolderItemStructure, Math.Min(successTargetFolderItemStructure.Length, 256))
                        };
                        bool isHeader = true;
                        long rowCount = 0;
                        foreach (var line in File.ReadLines(file, Encoding.GetEncoding("Shift_JIS")))
                        {
                            rowCount++;
                            if (isHeader)
                            {
                                isHeader = false;
                            }
                            else
                            {
                                // ファイルの行単位でループ
                                // 1行を配列に分割
                                var columns = line.Split(',');
                                header.Details.Add(new BaseRightHistoryDetailItemStructure
                                {
                                    DeleteFlag = 0,                                                 // 削除フラグ
                                    RegisteredUserCode = registrationUserID,                        // 登録担当者コード
                                    RegisteredDateTime = Convert.ToInt64(integrationDateTime),      // 登録日時
                                    RegisteredProgramID = registrationProgramId,                    // 登録プログラムID
                                    UpdatedUserCode = registrationUserID,                           // 更新担当者コード
                                    UpdatedDateTime = Convert.ToInt64(integrationDateTime),         // 更新日時
                                    UpdatedProgramID = registrationProgramId,                       // 更新プログラムID
                                    BaseRightNo = BRNo.ToString("D10"),                             // BR連携No
                                    ItemLineNo = Convert.ToInt32(rowCount),                         // BR連携品目構成行No

                                    parentItemNumber = columns.Length > 0 && columns[0] != null
                                      ? columns[0].Substring(0, Math.Min(columns[0].Length, 512))
                                      : string.Empty,                                               // 親品目番号
                                    balloonNumber = columns.Length > 0 && columns[1] != null
                                      ? columns[1].Substring(0, Math.Min(columns[1].Length, 512))
                                      : string.Empty,                                               // 風船番号(照番)
                                    order = columns.Length > 2 && columns[2] != null
                                      ? columns[2].Substring(0, Math.Min(columns[2].Length, 512))
                                      : string.Empty,                                               // 順序
                                    childItemNumber = columns.Length > 3 && columns[3] != null
                                      ? columns[3].Substring(0, Math.Min(columns[3].Length, 512))
                                      : string.Empty,                                               // 子品目番号
                                    quantity = columns.Length > 4 && columns[4] != null
                                      ? columns[4].Substring(0, Math.Min(columns[4].Length, 512))
                                      : string.Empty,                                               // 数量
                                });
                            }
                        }
                        //
                        header.Insert(connection);
                    }
                    #endregion

                    // *********************************************************************************
                    // * BaseRight連携履歴（設計変更）
                    // *********************************************************************************
                    #region BaseRight連携履歴（設計変更）
                    foreach (var file in filesDesignChange)
                    {
                        // 存在するファイル分だけループ
                        Console.WriteLine(file);
                        var BRNo = IdNumbering.UpdateCMSABNV99(registrationUserID, integrationDateTime, registrationProgramId, connection);
                        BaseRightHistoryHeader header = new BaseRightHistoryHeader
                        {
                            DeleteFlag = 0,                                                 // 削除フラグ
                            RegisteredUserCode = registrationUserID,                        // 登録担当者コード
                            RegisteredDateTime = Convert.ToInt64(integrationDateTime),      // 登録日時
                            RegisteredProgramID = registrationProgramId,                    // 登録プログラムID
                            UpdatedUserCode = registrationUserID,                           // 更新担当者コード
                            UpdatedDateTime = Convert.ToInt64(integrationDateTime),         // 更新日時
                            UpdatedProgramID = registrationProgramId,                       // 更新プログラムID
                            BaseRightNo = BRNo.ToString("D10"),                             // BR連携No
                            BaseRightCategory = "4",                                        // 4:設計変更
                            LinkedDate = Convert.ToInt64(integrationDateTime),
                            Sequence = 1,
                            Status = integrationErrorType_DesignChange is true
                                        ? "9"
                                        : "1",
                            Comment = afterIntegrationComment_DesignChange != null
                                      ? GetTrimmedString(afterIntegrationComment_DesignChange, Math.Min(afterIntegrationComment_DesignChange.Length, 256))
                                      : string.Empty,
                            SourceFilePath = file != null
                                      ? GetTrimmedString(file, Math.Min((file).Length, 256))
                                      : string.Empty,
                            DestinationFilePath = integrationErrorType_DesignChange is true
                                      ? GetTrimmedString(errorTargetFolderDesignChange, Math.Min(errorTargetFolderDesignChange.Length, 256))
                                      : GetTrimmedString(successTargetFolderDesignChange, Math.Min(successTargetFolderDesignChange.Length, 256))
                        };
                        bool isHeader = true;
                        long rowCount = 0;
                        using (var parser = new TextFieldParser(file, Encoding.GetEncoding("Shift_JIS")))
                        {
                            parser.TextFieldType = FieldType.Delimited;
                            parser.SetDelimiters(",");
                            parser.HasFieldsEnclosedInQuotes = true;

                            while (!parser.EndOfData)
                            {
                                string[] columns = parser.ReadFields();

                                // 改行除去（必要なら）
                                for (int i = 0; i < columns.Length; i++)
                                {
                                    columns[i] = columns[i]
                                        .Replace("\r", "")
                                        .Replace("\n", "");
                                }
                                rowCount++;
                                if (isHeader)
                                {
                                    isHeader = false;
                                }
                                else
                                {
                                    // ファイルの行単位でループ
                                    // 1行を配列に分割
                                    //var columns = SplitCsvLine(line);

                                    // 日付項目スラッシュを除く
                                    if (columns.Length > 20)
                                    {
                                        columns[20] = columns[20].Replace("/", "");
                                        columns[20] = columns[20].Replace(":", "");
                                        columns[20] = columns[20].Replace(" ", "");

                                    }
                                    if (columns.Length > 22)
                                    {
                                        columns[22] = columns[22].Replace("/", "");
                                        columns[22] = columns[22].Replace(":", "");
                                        columns[22] = columns[22].Replace(" ", "");
                                    }
                                    if (columns.Length > 24)
                                    {
                                        columns[24] = columns[24].Replace("/", "");
                                        columns[24] = columns[24].Replace(":", "");
                                        columns[24] = columns[24].Replace(" ", "");
                                    }

                                    header.Details.Add(new BaseRightHistoryDetailDesignChange
                                    {
                                        DeleteFlag = 0,                                                 // 削除フラグ
                                        RegisteredUserCode = registrationUserID,                        // 登録担当者コード
                                        RegisteredDateTime = Convert.ToInt64(integrationDateTime),      // 登録日時
                                        RegisteredProgramID = registrationProgramId,                    // 登録プログラムID
                                        UpdatedUserCode = registrationUserID,                           // 更新担当者コード
                                        UpdatedDateTime = Convert.ToInt64(integrationDateTime),         // 更新日時
                                        UpdatedProgramID = registrationProgramId,                       // 更新プログラムID
                                        BaseRightNo = BRNo.ToString("D10"),                             // BR連携No
                                        ItemLineNo = Convert.ToInt32(rowCount),                         // BR連携設計変更行No

                                        NotificationNumber = columns.Length > 0 && columns[0] != null
                                            ? columns[0].Substring(0, Math.Min(columns[0].Length, 512))
                                            : string.Empty,                                             //	設計変更通知書番号
                                        Classification = columns.Length > 1 && columns[1] != null
                                            ? columns[1].Substring(0, Math.Min(columns[1].Length, 512))
                                            : string.Empty,                                             //	区分
                                        CodeNumber = columns.Length > 2 && columns[2] != null
                                            ? columns[2].Substring(0, Math.Min(columns[2].Length, 512))
                                            : string.Empty,                                             //	コード番号
                                        Name = columns.Length > 3 && columns[3] != null
                                            ? columns[3].Substring(0, Math.Min(columns[3].Length, 512))
                                            : string.Empty,                                             //	名称
                                        Model = columns.Length > 4 && columns[4] != null
                                            ? columns[4].Substring(0, Math.Min(columns[4].Length, 512))
                                            : string.Empty,                                             //	機種
                                        NewItemNumber = columns.Length > 5 && columns[5] != null
                                            ? columns[5].Substring(0, Math.Min(columns[5].Length, 512))
                                            : string.Empty,                                             //	新品目番号
                                        OldItemNumber = columns.Length > 6 && columns[6] != null
                                            ? columns[6].Substring(0, Math.Min(columns[6].Length, 512))
                                            : string.Empty,                                             //	旧品目番号
                                        NewDrawingNumber = columns.Length > 7 && columns[7] != null
                                            ? columns[7].Substring(0, Math.Min(columns[7].Length, 512))
                                            : string.Empty,                                             //	新図面番号
                                        OldDrawingNumber = columns.Length > 8 && columns[8] != null
                                            ? columns[8].Substring(0, Math.Min(columns[8].Length, 512))
                                            : string.Empty,                                             //	旧図面番号
                                        Reason = columns.Length > 9 && columns[9] != null
                                            ? columns[9].Substring(0, Math.Min(columns[9].Length, 512))
                                            : string.Empty,                                             //	理由
                                        RevisionContactNo = columns.Length > 10 && columns[10] != null
                                            ? columns[10].Substring(0, Math.Min(columns[10].Length, 512))
                                            : string.Empty,                                             //	改定連絡所No.
                                        FeedbackSheetNo = columns.Length > 11 && columns[11] != null
                                            ? columns[11].Substring(0, Math.Min(columns[11].Length, 512))
                                            : string.Empty,                                             //	フィードバックシートNo.
                                        Item = columns.Length > 12 && columns[12] != null
                                            ? columns[12].Substring(0, Math.Min(columns[12].Length, 512))
                                            : string.Empty,                                             //	項目
                                        BOMChange = columns.Length > 13 && columns[13] != null
                                            ? columns[13].Substring(0, Math.Min(columns[13].Length, 512))
                                            : string.Empty,                                             //	BOM構成変更
                                        ProcessingChange = columns.Length > 14 && columns[14] != null
                                            ? columns[14].Substring(0, Math.Min(columns[14].Length, 512))
                                            : string.Empty,                                             //	加工変更(MC含む)
                                        SpecificationReview = columns.Length > 15 && columns[15] != null
                                            ? columns[15].Substring(0, Math.Min(columns[15].Length, 512))
                                            : string.Empty,                                             //	認定仕様範囲の再確認
                                        DocumentReplacement = columns.Length > 16 && columns[16] != null
                                            ? columns[16].Substring(0, Math.Min(columns[16].Length, 512))
                                            : string.Empty,                                             //	設計図書差し替え
                                        StockHandling = columns.Length > 17 && columns[17] != null
                                            ? columns[17].Substring(0, Math.Min(columns[17].Length, 512))
                                            : string.Empty,                                             //	仕掛品(H,B,S,A,KK番)・在庫品の処理
                                        ChangeReasonAndContent = columns.Length > 18 && columns[18] != null
                                            ? columns[18].Substring(0, Math.Min(columns[18].Length, 512))
                                            : string.Empty,                                             //	変更理由及び変更内容
                                        Creator = columns.Length > 19 && columns[19] != null
                                            ? columns[19].Substring(0, Math.Min(columns[19].Length, 512))
                                            : string.Empty,                                             //	作成者
                                        CreationDate = columns.Length > 20 && columns[20] != null
                                            ? columns[20].Substring(0, Math.Min(columns[20].Length, 512))
                                            : string.Empty,                                                       //	作成日
                                        Checker = columns.Length > 21 && columns[21] != null
                                            ? columns[21].Substring(0, Math.Min(columns[21].Length, 512))
                                            : string.Empty,                                             //	検図者
                                        CheckDate = columns.Length > 22 && columns[22] != null
                                            ? columns[22].Substring(0, Math.Min(columns[22].Length, 512))
                                            : string.Empty,                                                         //	検図日
                                        Approver = columns.Length > 23 && columns[23] != null
                                            ? columns[23].Substring(0, Math.Min(columns[23].Length, 512))
                                            : string.Empty,                                                //	承認者
                                        ApprovalDate = columns.Length > 24 && columns[24] != null
                                            ? columns[24].Substring(0, Math.Min(columns[24].Length, 512))
                                            : string.Empty,                                                     //	承認日
                                    });
                                }
                            }
                        }
                        //
                        header.Insert(connection);
                    }
                    #endregion
                    // *********************************************************************************
                    // * BaseRight連携履歴追加処理終了
                    // *********************************************************************************
                    // *********************************************************************************
                    // * ファイル移送処理
                    // *********************************************************************************
                    // *********************************************************************************
                    // * ファイル移送処理（品目連携ファイル）
                    // *********************************************************************************
                    #region ファイル移送処理（品目連携ファイル）
                    foreach (var file in filesItem)
                    {
                        try
                        {
                            // ファイル名を移動先フォルダのパスに設定
                            string fileName = Path.GetFileName(file);
                            string destinationPath = null;
                            if (integrationErrorType_Item)
                            {
                                destinationPath = Path.Combine(errorTargetFolderItem, fileName);
                            }
                            else
                            {
                                destinationPath = Path.Combine(successTargetFolderItem, fileName);
                            }

                            // 移動先に同名のファイルが存在する場合の処理
                            if (File.Exists(destinationPath))
                            {
                                // 上書きするために既存のファイルを削除
                                File.Delete(destinationPath);
                            }

                            // ファイルを移動
                            File.Move(file, destinationPath);
                            Console.WriteLine($"ファイルを移動しました: {fileName}");
                        }
                        catch (Exception ex)
                        {
                            // エラーハンドリング
                            Console.WriteLine($"ファイルの移動に失敗しました: {Path.GetFileName(file)} - {ex.Message}");
                            integrationErrorType_Item = true;
                        }
                    }
                    #endregion
                    // *********************************************************************************
                    // * ファイル移送処理（品目構成連携ファイル）
                    // *********************************************************************************
                    #region ファイル移送処理（品目構成連携ファイル）
                    foreach (var file in filesItemStructure)
                    {
                        try
                        {
                            // ファイル名を移動先フォルダのパスに設定
                            string fileName = Path.GetFileName(file);
                            string destinationPath = null;
                            if (integrationErrorType_ItemStructure)
                            {
                                destinationPath = Path.Combine(errorTargetFolderItemStructure, fileName);
                            }
                            else
                            {
                                destinationPath = Path.Combine(successTargetFolderItemStructure, fileName);
                            }

                            // 移動先に同名のファイルが存在する場合の処理
                            if (File.Exists(destinationPath))
                            {
                                // 上書きするために既存のファイルを削除
                                File.Delete(destinationPath);
                            }

                            // ファイルを移動
                            File.Move(file, destinationPath);
                            Console.WriteLine($"ファイルを移動しました: {fileName}");
                        }
                        catch (Exception ex)
                        {
                            // エラーハンドリング
                            Console.WriteLine($"ファイルの移動に失敗しました: {Path.GetFileName(file)} - {ex.Message}");
                            integrationErrorType_Item = true;
                        }
                    }
                    #endregion
                    // *********************************************************************************
                    // * ファイル移送処理（設計変更連携ファイル）
                    // *********************************************************************************
                    #region ファイル移送処理（設計変更連携ファイル）
                    foreach (var file in filesDesignChange)
                    {
                        try
                        {
                            // ファイル名を移動先フォルダのパスに設定
                            string fileName = Path.GetFileName(file);
                            string destinationPath = null;
                            if (integrationErrorType_DesignChange)
                            {
                                destinationPath = Path.Combine(errorTargetFolderDesignChange, fileName);
                            }
                            else
                            {
                                destinationPath = Path.Combine(successTargetFolderDesignChange, fileName);
                            }

                            // 移動先に同名のファイルが存在する場合の処理
                            if (File.Exists(destinationPath))
                            {
                                // 上書きするために既存のファイルを削除
                                File.Delete(destinationPath);
                            }

                            // ファイルを移動
                            File.Move(file, destinationPath);
                            Console.WriteLine($"ファイルを移動しました: {fileName}");
                        }
                        catch (Exception ex)
                        {
                            // エラーハンドリング
                            Console.WriteLine($"ファイルの移動に失敗しました: {Path.GetFileName(file)} - {ex.Message}");
                            integrationErrorType_Item = true;
                        }
                    }
                    #endregion
                }
                // *********************************************************************************
                // * データベース接続終了
                // *********************************************************************************
                IntegrationErrorSatus = integrationErrorType_Item || integrationErrorType_ItemStructure || integrationErrorType_DesignChange
                    ? "9"
                    : "1";

                // 値を設定
                IntegrationStatus.IntegrationErrorStatus = Convert.ToInt32(IntegrationErrorSatus);
                IntegrationStatus.afterIntegrationComment_Item = afterIntegrationComment_Item;
                IntegrationStatus.afterIntegrationComment_ItemStructure = afterIntegrationComment_ItemStructure;
                IntegrationStatus.afterIntegrationComment_DesignChange = afterIntegrationComment_DesignChange;

                // 戻り値を取得
                int returnCode = IntegrationStatus.GetReturnValue();
                Console.WriteLine("戻り値: " + returnCode);  // 出力: 戻り値: 3
#if DEBUG
                Console.WriteLine("画面を閉じるにはEnterを押下してください。");
                RTN = Console.ReadLine();
#endif
                return returnCode;
            }
            catch (OdbcException ex)
            {
                Console.WriteLine("接続エラー: " + ex.Message);
                return 3;
            }
            catch (Exception ex)
            {
                Console.WriteLine("エラーしました:" + ex.Message + " ");
                return 3;
            }
//            finally
//            {

////                Console.WriteLine("プログラムが終了しました。");
////#if DEBUG
////                Console.WriteLine("画面を閉じるにはEnterを押下してください。");
////                string RTN = Console.ReadLine();
////#endif
//            }
        }
        // Main終了
        // 以下サブモジュール／クラス

        static void GetMKUBN(OdbcConnection connection, string query, string envMode, string code, string value,
                    out string CodeDescription1 ,  out string CodeDescription2 )
        {
            // *********************************************************************************
            // * 汎用区分マスタから区分値を取得する
            // *********************************************************************************
            using (OdbcCommand command = new OdbcCommand(query, connection))
            {
                command.Parameters.AddWithValue("区分識別コード", code);
                command.Parameters.AddWithValue("区分値", value);
                CodeDescription1 = null;
                CodeDescription2 = null;
                using (OdbcDataReader reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        switch (envMode)
                        {
                            case "PRODUCTION":
                                CodeDescription1 = reader.GetString(1); // 文字値
                                CodeDescription2 = null;
                                break;
                            case "DEVELOPMENT":
                                CodeDescription1 = reader.GetString(2); // 文字値
                                CodeDescription2 = null;
                                break;
                            default:
                                CodeDescription1 = reader.GetString(1); // 文字値
                                CodeDescription2 = reader.GetString(2); // 文字値
                                break;
                        }
                    }
                }
            }
        }

        [AttributeUsage(AttributeTargets.Property)]
        public class CsvFieldAttribute : Attribute
        {
            // *********************************************************************************
            // * バリデーション属性クラス
            // *********************************************************************************
            public bool Required { get; set; }
            public int Length { get; set; }
            public Type DataType { get; set; }

            public CsvFieldAttribute(bool required, int length, Type dataType)
            {
                Required = required;
                Length = length;
                DataType = dataType;
            }
        }

        public class ItemRecord
        {
            // *********************************************************************************
            // * 品目仕様書データクラス
            // *********************************************************************************
            [CsvField(required: false, length: 13, dataType: typeof(string))] //図番/購入仕様書番号
            public string ItemDocumentNumber { get; set; }

            [CsvField(required: false, length: 3, dataType: typeof(string))] //改訂番号
            public int RevisionNumber { get; set; }

            [CsvField(required: false, length: 20, dataType: typeof(string))] //品目コード
            public string ItemCode { get; set; }

            [CsvField(required: false, length: 1024, dataType: typeof(string))] //品目名
            public string ItemName { get; set; }

            [CsvField(required: false, length: 128, dataType: typeof(string))] //品目英名
            public string ItemEnglishName { get; set; }

            [CsvField(required: false, length: 1, dataType: typeof(string))] //使用不可区分
            public string UnusableFlag { get; set; }

            [CsvField(required: false, length: 20, dataType: typeof(string))] //変更後品番
            public string NewItemNumber { get; set; }

            [CsvField(required: true, length: 30, dataType: typeof(string))] //クラス名
            public string ClassName { get; set; }

            [CsvField(required: true, length: 1, dataType: typeof(string))] //品番連携区分
            public string ItemLinkageFlag { get; set; }

            [CsvField(required: false, length: 10, dataType: typeof(string))] //組立区分
            public string AssemblyFlag { get; set; }

            [CsvField(required: false, length: 1024, dataType: typeof(string))] //品種コード
            public string VarietyCode { get; set; }

            [CsvField(required: false, length: 8, dataType: typeof(string))] //材質コード
            public string MaterialCode { get; set; }

            [CsvField(required: false, length: 200, dataType: typeof(string))] //備考
            public string Remarks { get; set; }
        }

        public class ItemStructureRecord
        {
            // *********************************************************************************
            // * 品目構成データファイルクラス
            // *********************************************************************************
            [CsvField(required: true, length: 20, dataType: typeof(string))] //親品目コード
            public string parentItemNumber { get; set; }

            [CsvField(required: false, length: 4, dataType: typeof(decimal))] //風船番号
            public int balloonNumber { get; set; }

            [CsvField(required: false, length: 4, dataType: typeof(decimal))] //品目構成行No
            public decimal ItemStructureLineNo { get; set; }

            [CsvField(required: true, length: 20, dataType: typeof(string))] //子品目コード
            public string childItemNumber { get; set; }

            [CsvField(required: true, length: 5, dataType: typeof(decimal))] //員数
            public decimal quantity { get; set; }
        }

        public class DesignChangeRecord
        {
            // *********************************************************************************
            // * 設計変更データファイルクラス
            // *********************************************************************************
            [CsvField(required: true, length: 9, dataType: typeof(string))] // 設計変更通知書番号
            public string NotificationNumber { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 区分
            public string Classification { get; set; }

            [CsvField(required: true, length: 4000, dataType: typeof(string))] // コード番号
            public string CodeNumber { get; set; }

            [CsvField(required: true, length: 50, dataType: typeof(string))] // 名称
            public string Name { get; set; }

            [CsvField(required: true, length: 20, dataType: typeof(string))] // 機種
            public string Model { get; set; }

            [CsvField(required: false, length: 20, dataType: typeof(string))] // 新品目番号
            public string NewItemNumber { get; set; }

            [CsvField(required: false, length: 20, dataType: typeof(string))] // 旧品目番号
            public string OldItemNumber { get; set; }

            [CsvField(required: false, length: 20, dataType: typeof(string))] // 新図面番号
            public string NewDrawingNumber { get; set; }

            [CsvField(required: false, length: 20, dataType: typeof(string))] // 旧図面番号
            public string OldDrawingNumber { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 理由
            public string Reason { get; set; }

            [CsvField(required: false, length: 20, dataType: typeof(string))] // 改定連絡所No.
            public string RevisionContactNo { get; set; }

            [CsvField(required: false, length: 15, dataType: typeof(string))] // フィードバックシートNo.
            public string FeedbackSheetNo { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 項目
            public string Item { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // BOM構成変更
            public string BOMChange { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 加工変更(MC含む)
            public string ProcessingChange { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 認定仕様範囲の再確認
            public string SpecificationReview { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 設計図書差し替え
            public string DocumentReplacement { get; set; }

            [CsvField(required: false, length: 4000, dataType: typeof(string))] // 仕掛品・在庫品の処理
            public string StockHandling { get; set; }

            [CsvField(required: true, length: 4000, dataType: typeof(string))] // 変更理由及び変更内容
            public string ChangeReasonAndContent { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 作成者
            public string Creator { get; set; }

            [CsvField(required: true, length: 14, dataType: typeof(long))] // 作成日
            public long CreationDate { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 検図者
            public string Checker { get; set; }

            [CsvField(required: true, length: 14, dataType: typeof(long))] // 検図日
            public long CheckDate { get; set; }

            [CsvField(required: true, length: 128, dataType: typeof(string))] // 承認者
            public string Approver { get; set; }

            [CsvField(required: true, length: 14, dataType: typeof(long))] // 承認日
            public long ApprovalDate { get; set; }
        }

        public static class CsvValidator
        {
            // *********************************************************************************
            // * 属性を使ったバリデーション処理
            // *********************************************************************************
            public static List<string> Validate<T>(string[] values)
            {
                var errors = new List<string>();
                var props = typeof(T).GetProperties();

                for (int i = 0; i < props.Length; i++)
                {
                    var prop = props[i];
                    var attr = prop.GetCustomAttribute<CsvFieldAttribute>();
                    var value = values.Length > i ? values[i] : "";

                    if (attr == null)
                        continue;

                    // 必須チェック
                    if (attr.Required && string.IsNullOrWhiteSpace(value))
                    {
                        errors.Add($"{prop.Name}: 必須項目です");
                        continue;
                    }

                    // 空なら以降のチェック不要
                    if (string.IsNullOrWhiteSpace(value))
                        continue;

                    // 桁数チェック
                    if (value.Length > attr.Length)
                    {
                        errors.Add($"{prop.Name}: 桁数オーバー（最大 {attr.Length} 桁）");
                    }

                    // 型チェック
                    if (attr.DataType == typeof(long) && !long.TryParse(value, out _))
                    {
                        errors.Add($"{prop.Name}: 数値ではありません");
                    }

                    // 型チェック
                    if (attr.DataType == typeof(decimal) && !decimal.TryParse(value, out _))
                    {
                        errors.Add($"{prop.Name}: 数値ではありません");
                    }

                    // 型チェック
                    if (attr.DataType == typeof(int) && !int.TryParse(value, out _))
                    {
                        errors.Add($"{prop.Name}: 数値ではありません");
                    }
                }
                return errors;
            }
        }

        public class ItemMasterRepository
        {
            // *********************************************************************************
            // * 品目マスタのエンティティクラス
            // *********************************************************************************
            private OdbcConnection _connection;
            private OdbcTransaction transaction;

            // コンストラクタ
            public ItemMasterRepository(OdbcConnection connection, OdbcTransaction transaction)
            {
                _connection = connection;
                this.transaction = transaction;
            }

            // ItemMasterクラスを定義
            public class ItemMaster
            {
                public decimal FGDELE { get; set; }					//削除フラグ
                public string SPIUSR { get; set; }                  //登録担当者コード
                public decimal SPIDTM { get; set; }                     //登録日時
                public string SPIPGM { get; set; }                  //登録プログラムID
                public string SPUUSR { get; set; }                  //更新担当者コード
                public decimal SPUDTM { get; set; }                     //更新日時
                public string SPUPGM { get; set; }                  //更新プログラムID
                public string CDHINM { get; set; }                  //品目コード
                public string NMHINM { get; set; }                  //品目名
                public string NRHINM { get; set; }                  //品目名略称
                public string NKHINM { get; set; }                  //品目カナ名
                public string ENHINM { get; set; }                  //品目英名
                public string ERHINM { get; set; }                  //品目英名略称
                public string CDHINS { get; set; }                  //品種コード
                public string NMHISJ { get; set; }                  //品種名称和文
                public string NMHISE { get; set; }                  //品種名称英文
                public string CDTANI { get; set; }                  //単位コード
                public string KBUISI { get; set; }                  //売上仕入使用区分
                public string KBCGHN { get; set; }                  //品目名称変更区分
                public string KBYOTO { get; set; }                  //用途区分
                public string KBKMTT { get; set; }                  //組立区分
                public string KBHNTY { get; set; }                  //品目タイプ区分
                public string CDHINB { get; set; }                  //品目分類コード
                public string CDHNGP { get; set; }                  //品目グループコード
                public decimal NOREVS { get; set; }                     //改訂番号
                public string KBSING { get; set; }                  //使用不可区分
                public string VLZUBN { get; set; }                  //図番/購入仕様書
                public string CDHHIN { get; set; }                  //変更後品目コード
                public string NMBCLS { get; set; }                  //Base-Rightクラス
                public string TXZUMN { get; set; }                  //図面番号特記事項
                public string TXBRTX { get; set; }                  //BR特記事項
                public string KBHREN { get; set; }                  //品番連携区分
                public string NOSOZU { get; set; }                  //素材図
                public string NOKIKN { get; set; }                  //木型・金型
                public string TXZAIS { get; set; }                  //材質
                public string CDZAIS1 { get; set; }                 //材質コード1
                public string CDZAIS2 { get; set; }                 //材質コード2
                public string CDZAIS3 { get; set; }                 //材質コード3
                public string CDZAIR { get; set; }                  //材料品目コード
                public string CDGKAS { get; set; }                  //外注加工先
                public string NOTANA { get; set; }                  //棚番
                public string CDKISH { get; set; }                  //代表型式コード
                public string KBZAIR { get; set; }                  //材料区分
                public string KBZAIK { get; set; }                  //在庫区分
                public string KBLTKR { get; set; }                  //ロット管理区分
                public string KBSESK { get; set; }                  //成績書扱い区分
                public string FGMILL { get; set; }                  //ミルシート要否
                public string FGKYOD { get; set; }                  //強度計算書有無
                public string FGKENT { get; set; }                  //検定要否
                public string FGSIJI { get; set; }                  //指示書要否
                public string FGZUMN { get; set; }                  //図面要否
                public string FGSHIY { get; set; }                  //仕様書要否
                public string FGNETU { get; set; }                  //熱処理記録要否
                public string FGSUNK { get; set; }                  //寸法確認書要否
                public string FGSETU { get; set; }                  //取扱説明書要否
                public string FGKHKG { get; set; }                  //KHK合格書要否
                public string FGKGSY { get; set; }                  //高圧ガス認定書要否
                public string FGPKAN { get; set; }                  //プロセス管理区分
                public string FGZKEN { get; set; }                  //材料検査有無
                public string FGTANS { get; set; }                  //単体出荷可否
                public string FGZTOS { get; set; }                  //材料時塗装要否
                public string FGZASK { get; set; }                  //材料支給要否
                public string FGTJIG { get; set; }                  //耐圧治具有無
                public string KBCYLI { get; set; }                  //新旧シリンダ区分
                public string KBGKAK { get; set; }                  //加工区分
                public string KBTEHI { get; set; }                  //手配区分
                public string KBNYTS { get; set; }                  //入荷管理対象区分
                public string KBNYKN { get; set; }                  //入荷検査要否区分
                public string KBKNSA { get; set; }                  //検査区分
                public decimal QTLOTS { get; set; }                 //ロット数量
                public decimal QTHCSU { get; set; }                 //最小発注数量
                public decimal QTIMOJ { get; set; }                 //鋳物単重
                public decimal PRIMOT { get; set; }                 //鋳物原価
                public string VLBSUN { get; set; }                  //部品寸法LWH(mm)
                public string TXBSUN { get; set; }                  //部品寸法特記事項
                public decimal VLSUN1 { get; set; }                 //寸法1
                public decimal VLSUN2 { get; set; }                 //寸法2
                public decimal VLSUN3 { get; set; }                 //寸法3
                public int QTLDTM { get; set; }                     //品目LT
                public string TXLTTM { get; set; }                  //品目LT特記事項
                public decimal QTBUHJ { get; set; }                 //部品単体重量(kg)
                public string TXBUHJ { get; set; }                  //部品単体重量特記事
                public decimal QTTNJU { get; set; }                 //単重
                public string CDDFSH { get; set; }                  //標準仕入先コード
                public string CDMEKA { get; set; }                  //メーカーコード
                public string CDDAIR { get; set; }                  //標準代理店コード
                public string TXMKAT { get; set; }                  //メーカー型番
                public string TXMEKA { get; set; }                  //メーカー特記事項
                public string TXBUHN { get; set; }                  //部品詳細内容
                public string TXB512 { get; set; }                  //特記事項
                public string CDSOKO { get; set; }                  //倉庫コード
                public string KBHNKZ { get; set; }                  //品目課税区分
                public string KBKEIG { get; set; }                  //軽減税率区分
                public decimal QTHCTN { get; set; }                 //発注点数
                public decimal QTTZAI { get; set; }                 //適正在庫数
                public int YMSIKO { get; set; }                     //仕込開始年月
                public int QTHNEN { get; set; }                     //平均算出年数
                public decimal RTKEPN { get; set; }                 //欠品許容率
                public string CDSRIS { get; set; }                  //シリーズコード
                public decimal VLSIZE { get; set; }                 //サイズ
                public string CDDCKA { get; set; }                  //債権貸方科目コード
                public string CDDDKA { get; set; }                  //債権借方科目コード
                public string CDICKA { get; set; }                  //債務(一般)貸方
                public string CDIDKA { get; set; }                  //債務(一般)借方
                public string CDSCKA { get; set; }                  //債務(製造)貸方
                public string CDSDKA { get; set; }                  //債務(製造)借方
                public decimal PRSANK { get; set; }                 //参考単価
                public string CDKASH { get; set; }                  //加工担当部門
                public decimal ATKKOS { get; set; }                 //加工工数
            }

            // 品目マスタをIDで取得するメソッド
            public ItemMaster GetItemById(string itemCode)
            {
                ItemMaster item = null;

                var query = @"
                SELECT *
                FROM MHINM
                WHERE CDHINM = ?"; // SQL読み込みクエリ

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("CDHINM", itemCode);
                    command.Transaction = this.transaction;

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            item = new ItemMaster
                            {

                                FGDELE = reader["FGDELE"] as decimal? ?? 0,
                                SPIUSR = reader["SPIUSR"].ToString(),
                                SPIDTM = reader["SPIDTM"] as decimal? ?? 0,
                                SPIPGM = reader["SPIPGM"].ToString(),
                                SPUUSR = reader["SPUUSR"].ToString(),
                                SPUDTM = reader["SPUDTM"] as decimal? ?? 0,
                                SPUPGM = reader["SPUPGM"].ToString(),
                                CDHINM = reader["CDHINM"].ToString(),
                                NMHINM = reader["NMHINM"].ToString(),
                                NRHINM = reader["NRHINM"]?.ToString() ?? null,
                                NKHINM = reader["NKHINM"]?.ToString() ?? null,
                                ENHINM = reader["ENHINM"]?.ToString() ?? null,
                                ERHINM = reader["ERHINM"]?.ToString() ?? null,
                                CDHINS = reader["CDHINS"]?.ToString() ?? null,
                                NMHISJ = reader["NMHISJ"]?.ToString() ?? null,
                                NMHISE = reader["NMHISE"]?.ToString() ?? null,
                                CDTANI = reader["CDTANI"]?.ToString() ?? null,
                                KBUISI = reader["KBUISI"]?.ToString() ?? null,
                                KBCGHN = reader["KBCGHN"]?.ToString() ?? null,
                                KBYOTO = reader["KBYOTO"]?.ToString() ?? null,
                                KBKMTT = reader["KBKMTT"]?.ToString() ?? null,
                                KBHNTY = reader["KBHNTY"]?.ToString() ?? null,
                                CDHINB = reader["CDHINB"]?.ToString() ?? null,
                                CDHNGP = reader["CDHNGP"]?.ToString() ?? null,
                                NOREVS = reader["NOREVS"] as decimal? ?? 0,
                                KBSING = reader["KBSING"]?.ToString() ?? null,
                                VLZUBN = reader["VLZUBN"]?.ToString() ?? null,
                                CDHHIN = reader["CDHHIN"]?.ToString() ?? null,
                                NMBCLS = reader["NMBCLS"]?.ToString() ?? null,
                                TXZUMN = reader["TXZUMN"]?.ToString() ?? null,
                                TXBRTX = reader["TXBRTX"]?.ToString() ?? null,
                                KBHREN = reader["KBHREN"]?.ToString() ?? null,
                                NOSOZU = reader["NOSOZU"]?.ToString() ?? null,
                                NOKIKN = reader["NOKIKN"]?.ToString() ?? null,
                                TXZAIS = reader["TXZAIS"]?.ToString() ?? null,
                                CDZAIS1 = reader["CDZAIS1"]?.ToString() ?? null,
                                CDZAIS2 = reader["CDZAIS2"]?.ToString() ?? null,
                                CDZAIS3 = reader["CDZAIS3"]?.ToString() ?? null,
                                CDZAIR = reader["CDZAIR"]?.ToString() ?? null,
                                CDGKAS = reader["CDGKAS"]?.ToString() ?? null,
                                NOTANA = reader["NOTANA"]?.ToString() ?? null,
                                CDKISH = reader["CDKISH"]?.ToString() ?? null,
                                KBZAIR = reader["KBZAIR"]?.ToString() ?? null,
                                KBZAIK = reader["KBZAIK"]?.ToString() ?? null,
                                KBLTKR = reader["KBLTKR"]?.ToString() ?? null,
                                KBSESK = reader["KBSESK"]?.ToString() ?? null,
                                FGMILL = reader["FGMILL"]?.ToString() ?? null,
                                FGKYOD = reader["FGKYOD"]?.ToString() ?? null,
                                FGKENT = reader["FGKENT"]?.ToString() ?? null,
                                FGSIJI = reader["FGSIJI"]?.ToString() ?? null,
                                FGZUMN = reader["FGZUMN"]?.ToString() ?? null,
                                FGSHIY = reader["FGSHIY"]?.ToString() ?? null,
                                FGNETU = reader["FGNETU"]?.ToString() ?? null,
                                FGSUNK = reader["FGSUNK"]?.ToString() ?? null,
                                FGSETU = reader["FGSETU"]?.ToString() ?? null,
                                FGKHKG = reader["FGKHKG"]?.ToString() ?? null,
                                FGKGSY = reader["FGKGSY"]?.ToString() ?? null,
                                FGPKAN = reader["FGPKAN"]?.ToString() ?? null,
                                FGZKEN = reader["FGZKEN"]?.ToString() ?? null,
                                FGTANS = reader["FGTANS"]?.ToString() ?? null,
                                FGZTOS = reader["FGZTOS"]?.ToString() ?? null,
                                FGZASK = reader["FGZASK"]?.ToString() ?? null,
                                FGTJIG = reader["FGTJIG"]?.ToString() ?? null,
                                KBCYLI = reader["KBCYLI"]?.ToString() ?? null,
                                KBGKAK = reader["KBGKAK"]?.ToString() ?? null,
                                KBTEHI = reader["KBTEHI"]?.ToString() ?? null,
                                KBNYTS = reader["KBNYTS"]?.ToString() ?? null,
                                KBNYKN = reader["KBNYKN"]?.ToString() ?? null,
                                KBKNSA = reader["KBKNSA"]?.ToString() ?? null,
                                QTLOTS = reader["QTLOTS"] as decimal? ?? 0,
                                QTHCSU = reader["QTHCSU"] as decimal? ?? 0,
                                QTIMOJ = reader["QTIMOJ"] as decimal? ?? 0,
                                PRIMOT = reader["PRIMOT"] as decimal? ?? 0,
                                VLBSUN = reader["VLBSUN"]?.ToString() ?? null,
                                TXBSUN = reader["TXBSUN"]?.ToString() ?? null,
                                VLSUN1 = reader["VLSUN1"] as decimal? ?? 0,
                                VLSUN2 = reader["VLSUN2"] as decimal? ?? 0,
                                VLSUN3 = reader["VLSUN3"] as decimal? ?? 0,
                                QTLDTM = reader["QTLDTM"] as int? ?? 0,
                                TXLTTM = reader["TXLTTM"]?.ToString() ?? null,
                                QTBUHJ = reader["QTBUHJ"] as decimal? ?? 0,
                                TXBUHJ = reader["TXBUHJ"]?.ToString() ?? null,
                                QTTNJU = reader["QTTNJU"] as decimal? ?? 0,
                                CDDFSH = reader["CDDFSH"]?.ToString() ?? null,
                                CDMEKA = reader["CDMEKA"]?.ToString() ?? null,
                                CDDAIR = reader["CDDAIR"]?.ToString() ?? null,
                                TXMKAT = reader["TXMKAT"]?.ToString() ?? null,
                                TXMEKA = reader["TXMEKA"]?.ToString() ?? null,
                                TXBUHN = reader["TXBUHN"]?.ToString() ?? null,
                                TXB512 = reader["TXB512"]?.ToString() ?? null,
                                CDSOKO = reader["CDSOKO"]?.ToString() ?? null,
                                KBHNKZ = reader["KBHNKZ"]?.ToString() ?? null,
                                KBKEIG = reader["KBKEIG"]?.ToString() ?? null,
                                QTHCTN = reader["QTHCTN"] as decimal? ?? 0,
                                QTTZAI = reader["QTTZAI"] as decimal? ?? 0,
                                YMSIKO = reader["YMSIKO"] as int? ?? 0,
                                QTHNEN = reader["QTHNEN"] as int? ?? 0,
                                RTKEPN = reader["RTKEPN"] as decimal? ?? 0,
                                CDSRIS = reader["CDSRIS"]?.ToString() ?? null,
                                VLSIZE = reader["VLSIZE"] as decimal? ?? 0,
                                CDDCKA = reader["CDDCKA"]?.ToString() ?? null,
                                CDDDKA = reader["CDDDKA"]?.ToString() ?? null,
                                CDICKA = reader["CDICKA"]?.ToString() ?? null,
                                CDIDKA = reader["CDIDKA"]?.ToString() ?? null,
                                CDSCKA = reader["CDSCKA"]?.ToString() ?? null,
                                CDSDKA = reader["CDSDKA"]?.ToString() ?? null,
                                PRSANK = reader["PRSANK"] as decimal? ?? 0,
                                CDKASH = reader["CDKASH"]?.ToString() ?? null,
                                ATKKOS = reader["ATKKOS"] as decimal? ?? 0
                            };
                        }
                    }
                }

                return item;
            }

            // 品目マスタを挿入するメソッド
            public void InsertItem(ItemMaster item)
            {
                var query = @"
                            INSERT 
                            INTO MHINM( 
                                FGDELE                                      -- 削除フラグ
                                , SPIUSR                                    -- 登録担当者コード
                                , SPIDTM                                    -- 登録日時
                                , SPIPGM                                    -- 登録プログラムID
                                , SPUUSR                                    -- 更新担当者コード
                                , SPUDTM                                    -- 更新日時
                                , SPUPGM                                    -- 更新プログラムID
                                , CDHINM                                    -- 品目コード
                                , NMHINM                                    -- 品目名
                                , NRHINM                                    -- 品目名略称
                                , NKHINM                                    -- 品目カナ名
                                , ENHINM                                    -- 品目英名
                                , ERHINM                                    -- 品目英名略称
                                , CDHINS                                    -- 品種コード
                                , NMHISJ                                    -- 品種名称和文
                                , NMHISE                                    -- 品種名称英文
                                , CDTANI                                    -- 単位コード
                                , KBUISI                                    -- 売上仕入使用区分
                                , KBCGHN                                    -- 品目名称変更区分
                                , KBYOTO                                    -- 用途区分
                                , KBKMTT                                    -- 組立区分
                                , KBHNTY                                    -- 品目タイプ区分
                                , CDHINB                                    -- 品目分類コード
                                , CDHNGP                                    -- 品目グループコード
                                , NOREVS                                    -- 改訂番号
                                , KBSING                                    -- 使用不可区分
                                , VLZUBN                                    -- 図番/購入仕様書
                                , CDHHIN                                    -- 変更後品目コード
                                , NMBCLS                                    -- Base-Rightクラス
                                , TXZUMN                                    -- 図面番号特記事項
                                , TXBRTX                                    -- BR特記事項
                                , KBHREN                                    -- 品番連携区分
                                , NOSOZU                                    -- 素材図
                                , NOKIKN                                    -- 木型・金型
                                , TXZAIS                                    -- 材質
                                , CDZAIS1                                   -- 材質コード1
                                , CDZAIS2                                   -- 材質コード2
                                , CDZAIS3                                   -- 材質コード3
                                , CDZAIR                                    -- 材料品目コード
                                , CDGKAS                                    -- 外注加工先
                                , NOTANA                                    -- 棚番
                                , CDKISH                                    -- 代表型式コード
                                , KBZAIR                                    -- 材料区分
                                , KBZAIK                                    -- 在庫区分
                                , KBLTKR                                    -- ロット管理区分
                                , KBSESK                                    -- 成績書扱い区分
                                , FGMILL                                    -- ミルシート要否
                                , FGKYOD                                    -- 強度計算書有無
                                , FGKENT                                    -- 検定要否
                                , FGSIJI                                    -- 指示書要否
                                , FGZUMN                                    -- 図面要否
                                , FGSHIY                                    -- 仕様書要否
                                , FGNETU                                    -- 熱処理記録要否
                                , FGSUNK                                    -- 寸法確認書要否
                                , FGSETU                                    -- 取扱説明書要否
                                , FGKHKG                                    -- KHK合格書要否
                                , FGKGSY                                    -- 高圧ガス認定書要否
                                , FGPKAN                                    -- プロセス管理区分
                                , FGZKEN                                    -- 材料検査有無
                                , FGTANS                                    -- 単体出荷可否
                                , FGZTOS                                    -- 材料時塗装要否
                                , FGZASK                                    -- 材料支給要否
                                , FGTJIG                                    -- 耐圧治具有無
                                , KBCYLI                                    -- 新旧シリンダ区分
                                , KBGKAK                                    -- 加工区分
                                , KBTEHI                                    -- 手配区分
                                , KBNYTS                                    -- 入荷管理対象区分
                                , KBNYKN                                    -- 入荷検査要否区分
                                , KBKNSA                                    -- 検査区分
                                , QTLOTS                                    -- ロット数量
                                , QTHCSU                                    -- 最小発注数量
                                , QTIMOJ                                    -- 鋳物単重
                                , PRIMOT                                    -- 鋳物原価
                                , VLBSUN                                    -- 部品寸法LWH(mm)
                                , TXBSUN                                    -- 部品寸法特記事項
                                , VLSUN1                                    -- 寸法1
                                , VLSUN2                                    -- 寸法2
                                , VLSUN3                                    -- 寸法3
                                , QTLDTM                                    -- 品目LT
                                , TXLTTM                                    -- 品目LT特記事項
                                , QTBUHJ                                    -- 部品単体重量(kg)
                                , TXBUHJ                                    -- 部品単体重量特記事
                                , QTTNJU                                    -- 単重
                                , CDDFSH                                    -- 標準仕入先コード
                                , CDMEKA                                    -- メーカーコード
                                , CDDAIR                                    -- 標準代理店コード
                                , TXMKAT                                    -- メーカー型番
                                , TXMEKA                                    -- メーカー特記事項
                                , TXBUHN                                    -- 部品詳細内容
                                , TXB512                                    -- 特記事項
                                , CDSOKO                                    -- 倉庫コード
                                , KBHNKZ                                    -- 品目課税区分
                                , KBKEIG                                    -- 軽減税率区分
                                , QTHCTN                                    -- 発注点数
                                , QTTZAI                                    -- 適正在庫数
                                , YMSIKO                                    -- 仕込開始年月
                                , QTHNEN                                    -- 平均算出年数
                                , RTKEPN                                    -- 欠品許容率
                                , CDSRIS                                    -- シリーズコード
                                , VLSIZE                                    -- サイズ
                                , CDDCKA                                    -- 債権貸方科目コード
                                , CDDDKA                                    -- 債権借方科目コード
                                , CDICKA                                    -- 債務(一般)貸方
                                , CDIDKA                                    -- 債務(一般)借方
                                , CDSCKA                                    -- 債務(製造)貸方
                                , CDSDKA                                    -- 債務(製造)借方
                                , PRSANK                                    -- 参考単価
                                , CDKASH                                    -- 加工担当部門
                                , ATKKOS                                    -- 加工工数
                            ) 
                            VALUES ( 
                                  ?                                     -- 削除フラグ
                                , ?                                   -- 登録担当者コード
                                , ?                                   -- 登録日時
                                , ?                                   -- 登録プログラムID
                                , ?                                   -- 更新担当者コード
                                , ?                                   -- 更新日時
                                , ?                                   -- 更新プログラムID
                                , ?                                   -- 品目コード
                                , ?                                   -- 品目名
                                , ?                                   -- 品目名略称
                                , ?                                   -- 品目カナ名
                                , ?                                   -- 品目英名
                                , ?                                   -- 品目英名略称
                                , ?                                   -- 品種コード
                                , ?                                   -- 品種名称和文
                                , ?                                   -- 品種名称英文
                                , ?                                   -- 単位コード
                                , ?                                   -- 売上仕入使用区分
                                , ?                                   -- 品目名称変更区分
                                , ?                                   -- 用途区分
                                , ?                                   -- 組立区分
                                , ?                                   -- 品目タイプ区分
                                , ?                                   -- 品目分類コード
                                , ?                                   -- 品目グループコード
                                , ?                                   -- 改訂番号
                                , ?                                   -- 使用不可区分
                                , ?                                   -- 図番/購入仕様書
                                , ?                                   -- 変更後品目コード
                                , ?                                   -- Base-Rightクラス
                                , ?                                   -- 図面番号特記事項
                                , ?                                   -- BR特記事項
                                , ?                                   -- 品番連携区分
                                , ?                                   -- 素材図
                                , ?                                   -- 木型・金型
                                , ?                                   -- 材質
                                , ?                                  -- 材質コード1
                                , ?                                  -- 材質コード2
                                , ?                                  -- 材質コード3
                                , ?                                   -- 材料品目コード
                                , ?                                   -- 外注加工先
                                , ?                                   -- 棚番
                                , ?                                   -- 代表型式コード
                                , ?                                   -- 材料区分
                                , ?                                   -- 在庫区分
                                , ?                                   -- ロット管理区分
                                , ?                                   -- 成績書扱い区分
                                , ?                                   -- ミルシート要否
                                , ?                                   -- 強度計算書有無
                                , ?                                   -- 検定要否
                                , ?                                   -- 指示書要否
                                , ?                                   -- 図面要否
                                , ?                                   -- 仕様書要否
                                , ?                                   -- 熱処理記録要否
                                , ?                                   -- 寸法確認書要否
                                , ?                                   -- 取扱説明書要否
                                , ?                                   -- KHK合格書要否
                                , ?                                   -- 高圧ガス認定書要否
                                , ?                                   -- プロセス管理区分
                                , ?                                   -- 材料検査有無
                                , ?                                   -- 単体出荷可否
                                , ?                                   -- 材料時塗装要否
                                , ?                                   -- 材料支給要否
                                , ?                                   -- 耐圧治具有無
                                , ?                                   -- 新旧シリンダ区分
                                , ?                                   -- 加工区分
                                , ?                                   -- 手配区分
                                , ?                                   -- 入荷管理対象区分
                                , ?                                   -- 入荷検査要否区分
                                , ?                                   -- 検査区分
                                , ?                                   -- ロット数量
                                , ?                                   -- 最小発注数量
                                , ?                                   -- 鋳物単重
                                , ?                                   -- 鋳物原価
                                , ?                                   -- 部品寸法LWH(mm)
                                , ?                                   -- 部品寸法特記事項
                                , ?                                   -- 寸法1
                                , ?                                   -- 寸法2
                                , ?                                   -- 寸法3
                                , ?                                   -- 品目LT
                                , ?                                   -- 品目LT特記事項
                                , ?                                   -- 部品単体重量(kg)
                                , ?                                   -- 部品単体重量特記事
                                , ?                                   -- 単重
                                , ?                                   -- 標準仕入先コード
                                , ?                                   -- メーカーコード
                                , ?                                   -- 標準代理店コード
                                , ?                                   -- メーカー型番
                                , ?                                   -- メーカー特記事項
                                , ?                                   -- 部品詳細内容
                                , ?                                   -- 特記事項
                                , ?                                   -- 倉庫コード
                                , ?                                   -- 品目課税区分
                                , ?                                   -- 軽減税率区分
                                , ?                                   -- 発注点数
                                , ?                                   -- 適正在庫数
                                , ?                                   -- 仕込開始年月
                                , ?                                   -- 平均算出年数
                                , ?                                   -- 欠品許容率
                                , ?                                   -- シリーズコード
                                , ?                                   -- サイズ
                                , ?                                   -- 債権貸方科目コード
                                , ?                                   -- 債権借方科目コード
                                , ?                                   -- 債務(一般)貸方
                                , ?                                   -- 債務(一般)借方
                                , ?                                   -- 債務(製造)貸方
                                , ?                                   -- 債務(製造)借方
                                , ?                                   -- 参考単価
                                , ?                                   -- 加工担当部門
                                , ?                                   -- 加工工数
                            )"; // SQL挿入クエリ

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", Convert.ToDecimal(item.FGDELE));
                    command.Parameters.AddWithValue("@SPIUSR", string.IsNullOrEmpty(item.SPIUSR) ? (object)DBNull.Value : item.SPIUSR);
                    command.Parameters.AddWithValue("@SPIDTM", Convert.ToDecimal(item.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPIPGM", string.IsNullOrEmpty(item.SPIPGM) ? (object)DBNull.Value : item.SPIPGM);
                    command.Parameters.AddWithValue("@SPUUSR", string.IsNullOrEmpty(item.SPUUSR) ? (object)DBNull.Value : item.SPUUSR);
                    command.Parameters.AddWithValue("@SPUDTM", Convert.ToDecimal(item.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPUPGM", string.IsNullOrEmpty(item.SPUPGM) ? (object)DBNull.Value : item.SPUPGM);
                    command.Parameters.AddWithValue("@CDHINM", string.IsNullOrEmpty(item.CDHINM) ? (object)DBNull.Value : item.CDHINM);
                    command.Parameters.AddWithValue("@NMHINM", string.IsNullOrEmpty(item.NMHINM) ? (object)DBNull.Value : item.NMHINM);
                    command.Parameters.AddWithValue("@NRHINM", string.IsNullOrEmpty(item.NRHINM) ? (object)DBNull.Value : item.NRHINM);
                    command.Parameters.AddWithValue("@NKHINM", string.IsNullOrEmpty(item.NKHINM) ? (object)DBNull.Value : item.NKHINM);
                    command.Parameters.AddWithValue("@ENHINM", string.IsNullOrEmpty(item.ENHINM) ? (object)DBNull.Value : item.ENHINM);
                    command.Parameters.AddWithValue("@ERHINM", string.IsNullOrEmpty(item.ERHINM) ? (object)DBNull.Value : item.ERHINM);
                    command.Parameters.AddWithValue("@CDHINS", string.IsNullOrEmpty(item.CDHINS) ? (object)DBNull.Value : item.CDHINS);
                    command.Parameters.AddWithValue("@NMHISJ", string.IsNullOrEmpty(item.NMHISJ) ? (object)DBNull.Value : item.NMHISJ);
                    command.Parameters.AddWithValue("@NMHISE", string.IsNullOrEmpty(item.NMHISE) ? (object)DBNull.Value : item.NMHISE);
                    command.Parameters.AddWithValue("@CDTANI", string.IsNullOrEmpty(item.CDTANI) ? (object)DBNull.Value : item.CDTANI);
                    command.Parameters.AddWithValue("@KBUISI", string.IsNullOrEmpty(item.KBUISI) ? (object)DBNull.Value : item.KBUISI);
                    command.Parameters.AddWithValue("@KBCGHN", string.IsNullOrEmpty(item.KBCGHN) ? (object)DBNull.Value : item.KBCGHN);
                    command.Parameters.AddWithValue("@KBYOTO", string.IsNullOrEmpty(item.KBYOTO) ? (object)DBNull.Value : item.KBYOTO);
                    command.Parameters.AddWithValue("@KBKMTT", string.IsNullOrEmpty(item.KBKMTT) ? (object)DBNull.Value : item.KBKMTT);
                    command.Parameters.AddWithValue("@KBHNTY", string.IsNullOrEmpty(item.KBHNTY) ? (object)DBNull.Value : item.KBHNTY);
                    command.Parameters.AddWithValue("@CDHINB", string.IsNullOrEmpty(item.CDHINB) ? (object)DBNull.Value : item.CDHINB);
                    command.Parameters.AddWithValue("@CDHNGP", string.IsNullOrEmpty(item.CDHNGP) ? (object)DBNull.Value : item.CDHNGP);
                    command.Parameters.AddWithValue("@NOREVS", Convert.ToDecimal(item.NOREVS)); // int への適切な変換
                    command.Parameters.AddWithValue("@KBSING", string.IsNullOrEmpty(item.KBSING) ? (object)DBNull.Value : item.KBSING);
                    command.Parameters.AddWithValue("@VLZUBN", string.IsNullOrEmpty(item.VLZUBN) ? (object)DBNull.Value : item.VLZUBN);
                    command.Parameters.AddWithValue("@CDHHIN", string.IsNullOrEmpty(item.CDHHIN) ? (object)DBNull.Value : item.CDHHIN);
                    command.Parameters.AddWithValue("@NMBCLS", string.IsNullOrEmpty(item.NMBCLS) ? (object)DBNull.Value : item.NMBCLS);
                    command.Parameters.AddWithValue("@TXZUMN", string.IsNullOrEmpty(item.TXZUMN) ? (object)DBNull.Value : item.TXZUMN);
                    command.Parameters.AddWithValue("@TXBRTX", string.IsNullOrEmpty(item.TXBRTX) ? (object)DBNull.Value : item.TXBRTX);
                    command.Parameters.AddWithValue("@KBHREN", string.IsNullOrEmpty(item.KBHREN) ? (object)DBNull.Value : item.KBHREN);
                    command.Parameters.AddWithValue("@NOSOZU", string.IsNullOrEmpty(item.NOSOZU) ? (object)DBNull.Value : item.NOSOZU);
                    command.Parameters.AddWithValue("@NOKIKN", string.IsNullOrEmpty(item.NOKIKN) ? (object)DBNull.Value : item.NOKIKN);
                    command.Parameters.AddWithValue("@TXZAIS", string.IsNullOrEmpty(item.TXZAIS) ? (object)DBNull.Value : item.TXZAIS);
                    command.Parameters.AddWithValue("@CDZAIS1", string.IsNullOrEmpty(item.CDZAIS1) ? (object)DBNull.Value : item.CDZAIS1);
                    command.Parameters.AddWithValue("@CDZAIS2", string.IsNullOrEmpty(item.CDZAIS2) ? (object)DBNull.Value : item.CDZAIS2);
                    command.Parameters.AddWithValue("@CDZAIS3", string.IsNullOrEmpty(item.CDZAIS3) ? (object)DBNull.Value : item.CDZAIS3);
                    command.Parameters.AddWithValue("@CDZAIR", string.IsNullOrEmpty(item.CDZAIR) ? (object)DBNull.Value : item.CDZAIR);
                    command.Parameters.AddWithValue("@CDGKAS", string.IsNullOrEmpty(item.CDGKAS) ? (object)DBNull.Value : item.CDGKAS);
                    command.Parameters.AddWithValue("@NOTANA", string.IsNullOrEmpty(item.NOTANA) ? (object)DBNull.Value : item.NOTANA);
                    command.Parameters.AddWithValue("@CDKISH", string.IsNullOrEmpty(item.CDKISH) ? (object)DBNull.Value : item.CDKISH);
                    command.Parameters.AddWithValue("@KBZAIR", string.IsNullOrEmpty(item.KBZAIR) ? (object)DBNull.Value : item.KBZAIR);
                    command.Parameters.AddWithValue("@KBZAIK", string.IsNullOrEmpty(item.KBZAIK) ? (object)DBNull.Value : item.KBZAIK);
                    command.Parameters.AddWithValue("@KBLTKR", string.IsNullOrEmpty(item.KBLTKR) ? (object)DBNull.Value : item.KBLTKR);
                    command.Parameters.AddWithValue("@KBSESK", string.IsNullOrEmpty(item.KBSESK) ? (object)DBNull.Value : item.KBSESK);
                    command.Parameters.AddWithValue("@FGMILL", string.IsNullOrEmpty(item.FGMILL) ? (object)DBNull.Value : item.FGMILL);
                    command.Parameters.AddWithValue("@FGKYOD", string.IsNullOrEmpty(item.FGKYOD) ? (object)DBNull.Value : item.FGKYOD);
                    command.Parameters.AddWithValue("@FGKENT", string.IsNullOrEmpty(item.FGKENT) ? (object)DBNull.Value : item.FGKENT);
                    command.Parameters.AddWithValue("@FGSIJI", string.IsNullOrEmpty(item.FGSIJI) ? (object)DBNull.Value : item.FGSIJI);
                    command.Parameters.AddWithValue("@FGZUMN", string.IsNullOrEmpty(item.FGZUMN) ? (object)DBNull.Value : item.FGZUMN);
                    command.Parameters.AddWithValue("@FGSHIY", string.IsNullOrEmpty(item.FGSHIY) ? (object)DBNull.Value : item.FGSHIY);
                    command.Parameters.AddWithValue("@FGNETU", string.IsNullOrEmpty(item.FGNETU) ? (object)DBNull.Value : item.FGNETU);
                    command.Parameters.AddWithValue("@FGSUNK", string.IsNullOrEmpty(item.FGSUNK) ? (object)DBNull.Value : item.FGSUNK);
                    command.Parameters.AddWithValue("@FGSETU", string.IsNullOrEmpty(item.FGSETU) ? (object)DBNull.Value : item.FGSETU);
                    command.Parameters.AddWithValue("@FGKHKG", string.IsNullOrEmpty(item.FGKHKG) ? (object)DBNull.Value : item.FGKHKG);
                    command.Parameters.AddWithValue("@FGKGSY", string.IsNullOrEmpty(item.FGKGSY) ? (object)DBNull.Value : item.FGKGSY);
                    command.Parameters.AddWithValue("@FGPKAN", string.IsNullOrEmpty(item.FGPKAN) ? (object)DBNull.Value : item.FGPKAN);
                    command.Parameters.AddWithValue("@FGZKEN", string.IsNullOrEmpty(item.FGZKEN) ? (object)DBNull.Value : item.FGZKEN);
                    command.Parameters.AddWithValue("@FGTANS", string.IsNullOrEmpty(item.FGTANS) ? (object)DBNull.Value : item.FGTANS);
                    command.Parameters.AddWithValue("@FGZTOS", string.IsNullOrEmpty(item.FGZTOS) ? (object)DBNull.Value : item.FGZTOS);
                    command.Parameters.AddWithValue("@FGZASK", string.IsNullOrEmpty(item.FGZASK) ? (object)DBNull.Value : item.FGZASK);
                    command.Parameters.AddWithValue("@FGTJIG", string.IsNullOrEmpty(item.FGTJIG) ? (object)DBNull.Value : item.FGTJIG);
                    command.Parameters.AddWithValue("@KBCYLI", string.IsNullOrEmpty(item.KBCYLI) ? (object)DBNull.Value : item.KBCYLI);
                    command.Parameters.AddWithValue("@KBGKAK", string.IsNullOrEmpty(item.KBGKAK) ? (object)DBNull.Value : item.KBGKAK);
                    command.Parameters.AddWithValue("@KBTEHI", string.IsNullOrEmpty(item.KBTEHI) ? (object)DBNull.Value : item.KBTEHI);
                    command.Parameters.AddWithValue("@KBNYTS", string.IsNullOrEmpty(item.KBNYTS) ? (object)DBNull.Value : item.KBNYTS);
                    command.Parameters.AddWithValue("@KBNYKN", string.IsNullOrEmpty(item.KBNYKN) ? (object)DBNull.Value : item.KBNYKN);
                    command.Parameters.AddWithValue("@KBKNSA", string.IsNullOrEmpty(item.KBKNSA) ? (object)DBNull.Value : item.KBKNSA);
                    command.Parameters.AddWithValue("@QTLOTS", Convert.ToDecimal(item.QTLOTS));
                    command.Parameters.AddWithValue("@QTHCSU", Convert.ToDecimal(item.QTHCSU));
                    command.Parameters.AddWithValue("@QTIMOJ", Convert.ToDecimal(item.QTIMOJ)); // デフォルト値
                    command.Parameters.AddWithValue("@PRIMOT", Convert.ToDecimal(item.PRIMOT)); // デフォルト値
                    command.Parameters.AddWithValue("@VLBSUN", string.IsNullOrEmpty(item.VLBSUN) ? (object)DBNull.Value : item.VLBSUN);
                    command.Parameters.AddWithValue("@TXBSUN", string.IsNullOrEmpty(item.TXBSUN) ? (object)DBNull.Value : item.TXBSUN);
                    command.Parameters.AddWithValue("@VLSUN1", Convert.ToDecimal(item.VLSUN1)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN2", Convert.ToDecimal(item.VLSUN2)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN3", Convert.ToDecimal(item.VLSUN3)); // デフォルト値
                    command.Parameters.AddWithValue("@QTLDTM", Convert.ToDecimal(item.QTLDTM));
                    command.Parameters.AddWithValue("@TXLTTM", string.IsNullOrEmpty(item.TXLTTM) ? (object)DBNull.Value : item.TXLTTM);
                    command.Parameters.AddWithValue("@QTBUHJ", Convert.ToDecimal(item.QTBUHJ)); // デフォルト値
                    command.Parameters.AddWithValue("@TXBUHJ", string.IsNullOrEmpty(item.TXBUHJ) ? (object)DBNull.Value : item.TXBUHJ);
                    command.Parameters.AddWithValue("@QTTNJU", Convert.ToDecimal(item.QTTNJU)); // デフォルト値
                    command.Parameters.AddWithValue("@CDDFSH", string.IsNullOrEmpty(item.CDDFSH) ? (object)DBNull.Value : item.CDDFSH);
                    command.Parameters.AddWithValue("@CDMEKA", string.IsNullOrEmpty(item.CDMEKA) ? (object)DBNull.Value : item.CDMEKA);
                    command.Parameters.AddWithValue("@CDDAIR", string.IsNullOrEmpty(item.CDDAIR) ? (object)DBNull.Value : item.CDDAIR);
                    command.Parameters.AddWithValue("@TXMKAT", string.IsNullOrEmpty(item.TXMKAT) ? (object)DBNull.Value : item.TXMKAT);
                    command.Parameters.AddWithValue("@TXMEKA", string.IsNullOrEmpty(item.TXMEKA) ? (object)DBNull.Value : item.TXMEKA);
                    command.Parameters.AddWithValue("@TXBUHN", string.IsNullOrEmpty(item.TXBUHN) ? (object)DBNull.Value : item.TXBUHN);
                    command.Parameters.AddWithValue("@TXB512", string.IsNullOrEmpty(item.TXB512) ? (object)DBNull.Value : item.TXB512);
                    command.Parameters.AddWithValue("@CDSOKO", string.IsNullOrEmpty(item.CDSOKO) ? (object)DBNull.Value : item.CDSOKO);
                    command.Parameters.AddWithValue("@KBHNKZ", string.IsNullOrEmpty(item.KBHNKZ) ? (object)DBNull.Value : item.KBHNKZ);
                    command.Parameters.AddWithValue("@KBKEIG", string.IsNullOrEmpty(item.KBKEIG) ? (object)DBNull.Value : item.KBKEIG);
                    command.Parameters.AddWithValue("@QTHCTN", Convert.ToDecimal(item.QTHCTN)); // デフォルト値
                    command.Parameters.AddWithValue("@QTTZAI", Convert.ToDecimal(item.QTTZAI)); // デフォルト値
                    command.Parameters.AddWithValue("@YMSIKO", Convert.ToDecimal(item.YMSIKO));
                    command.Parameters.AddWithValue("@QTHNEN", Convert.ToDecimal(item.QTHNEN)); // デフォルト値
                    command.Parameters.AddWithValue("@RTKEPN", Convert.ToDecimal(item.RTKEPN)); // デフォルト値
                    command.Parameters.AddWithValue("@CDSRIS", string.IsNullOrEmpty(item.CDSRIS) ? (object)DBNull.Value : item.CDSRIS);
                    command.Parameters.AddWithValue("@VLSIZE", Convert.ToDecimal(item.VLSIZE));
                    command.Parameters.AddWithValue("@CDDCKA", string.IsNullOrEmpty(item.CDDCKA) ? (object)DBNull.Value : item.CDDCKA);
                    command.Parameters.AddWithValue("@CDDDKA", string.IsNullOrEmpty(item.CDDDKA) ? (object)DBNull.Value : item.CDDDKA);
                    command.Parameters.AddWithValue("@CDICKA", string.IsNullOrEmpty(item.CDICKA) ? (object)DBNull.Value : item.CDICKA);
                    command.Parameters.AddWithValue("@CDIDKA", string.IsNullOrEmpty(item.CDIDKA) ? (object)DBNull.Value : item.CDIDKA);
                    command.Parameters.AddWithValue("@CDSCKA", string.IsNullOrEmpty(item.CDSCKA) ? (object)DBNull.Value : item.CDSCKA);
                    command.Parameters.AddWithValue("@CDSDKA", string.IsNullOrEmpty(item.CDSDKA) ? (object)DBNull.Value : item.CDSDKA);
                    command.Parameters.AddWithValue("@PRSANK", Convert.ToDecimal(item.PRSANK)); // デフォルト値
                    command.Parameters.AddWithValue("@CDKASH", string.IsNullOrEmpty(item.CDKASH) ? (object)DBNull.Value : item.CDKASH);
                    command.Parameters.AddWithValue("@ATKKOS", Convert.ToDecimal(item.ATKKOS)); // デフォルト値
                    //command.Parameters.AddWithValue("@CDHINM", item.CDHINM); // 条件用の品目コードを追加

                    command.Transaction = this.transaction;
                    command.ExecuteNonQuery(); // 挿入を実行
                }
            }
            // 品目マスタを更新するメソッド
            public void UpdateItem(ItemMaster item)
            {
                var query = @"
                        UPDATE MHINM 
                        SET
                              FGDELE = ?                        -- 削除フラグ
                            , SPIUSR = ?                        -- 登録担当者コード
                            , SPIDTM = ?                        -- 登録日時
                            , SPIPGM = ?                        -- 登録プログラムID
                            , SPUUSR = ?                        -- 更新担当者コード
                            , SPUDTM = ?                        -- 更新日時
                            , SPUPGM = ?                        -- 更新プログラムID
                            , NMHINM = ?                        -- 品目名
                            , NRHINM = ?                        -- 品目名略称
                            , NKHINM = ?                        -- 品目カナ名
                            , ENHINM = ?                        -- 品目英名
                            , ERHINM = ?                        -- 品目英名略称
                            , CDHINS = ?                        -- 品種コード
                            , NMHISJ = ?                        -- 品種名称和文
                            , NMHISE = ?                        -- 品種名称英文
                            , CDTANI = ?                        -- 単位コード
                            , KBUISI = ?                        -- 売上仕入使用区分
                            , KBCGHN = ?                        -- 品目名称変更区分
                            , KBYOTO = ?                        -- 用途区分
                            , KBKMTT = ?                        -- 組立区分
                            , KBHNTY = ?                        -- 品目タイプ区分
                            , CDHINB = ?                        -- 品目分類コード
                            , CDHNGP = ?                        -- 品目グループコード
                            , NOREVS = ?                        -- 改訂番号
                            , KBSING = ?                        -- 使用不可区分
                            , VLZUBN = ?                        -- 図番/購入仕様書
                            , CDHHIN = ?                        -- 変更後品目コード
                            , NMBCLS = ?                        -- Base-Rightクラス
                            , TXZUMN = ?                        -- 図面番号特記事項
                            , TXBRTX = ?                        -- BR特記事項
                            , KBHREN = ?                        -- 品番連携区分
                            , NOSOZU = ?                        -- 素材図
                            , NOKIKN = ?                        -- 木型・金型
                            , TXZAIS = ?                        -- 材質
                            , CDZAIS1 =?                        -- 材質コード1
                            , CDZAIS2 =?                        -- 材質コード2
                            , CDZAIS3 =?                        -- 材質コード3
                            , CDZAIR = ?                        -- 材料品目コード
                            , CDGKAS = ?                        -- 外注加工先
                            , NOTANA = ?                        -- 棚番
                            , CDKISH = ?                        -- 代表型式コード
                            , KBZAIR = ?                        -- 材料区分
                            , KBZAIK = ?                        -- 在庫区分
                            , KBLTKR = ?                        -- ロット管理区分
                            , KBSESK = ?                        -- 成績書扱い区分
                            , FGMILL = ?                        -- ミルシート要否
                            , FGKYOD = ?                        -- 強度計算書有無
                            , FGKENT = ?                        -- 検定要否
                            , FGSIJI = ?                        -- 指示書要否
                            , FGZUMN = ?                        -- 図面要否
                            , FGSHIY = ?                        -- 仕様書要否
                            , FGNETU = ?                        -- 熱処理記録要否
                            , FGSUNK = ?                        -- 寸法確認書要否
                            , FGSETU = ?                        -- 取扱説明書要否
                            , FGKHKG = ?                        -- KHK合格書要否
                            , FGKGSY = ?                        -- 高圧ガス認定書要否
                            , FGPKAN = ?                        -- プロセス管理区分
                            , FGZKEN = ?                        -- 材料検査有無
                            , FGTANS = ?                        -- 単体出荷可否
                            , FGZTOS = ?                        -- 材料時塗装要否
                            , FGZASK = ?                        -- 材料支給要否
                            , FGTJIG = ?                        -- 耐圧治具有無
                            , KBCYLI = ?                        -- 新旧シリンダ区分
                            , KBGKAK = ?                        -- 加工区分
                            , KBTEHI = ?                        -- 手配区分
                            , KBNYTS = ?                        -- 入荷管理対象区分
                            , KBNYKN = ?                        -- 入荷検査要否区分
                            , KBKNSA = ?                        -- 検査区分
                            , QTLOTS = ?                        -- ロット数量
                            , QTHCSU = ?                        -- 最小発注数量
                            , QTIMOJ = ?                        -- 鋳物単重
                            , PRIMOT = ?                        -- 鋳物原価
                            , VLBSUN = ?                        -- 部品寸法LWH(mm)
                            , TXBSUN = ?                        -- 部品寸法特記事項
                            , VLSUN1 = ?                        -- 寸法1
                            , VLSUN2 = ?                        -- 寸法2
                            , VLSUN3 = ?                        -- 寸法3
                            , QTLDTM = ?                        -- 品目LT
                            , TXLTTM = ?                        -- 品目LT特記事項
                            , QTBUHJ = ?                        -- 部品単体重量(kg)
                            , TXBUHJ = ?                        -- 部品単体重量特記事
                            , QTTNJU = ?                        -- 単重
                            , CDDFSH = ?                        -- 標準仕入先コード
                            , CDMEKA = ?                        -- メーカーコード
                            , CDDAIR = ?                        -- 標準代理店コード
                            , TXMKAT = ?                        -- メーカー型番
                            , TXMEKA = ?                        -- メーカー特記事項
                            , TXBUHN = ?                        -- 部品詳細内容
                            , TXB512 = ?                        -- 特記事項
                            , CDSOKO = ?                        -- 倉庫コード
                            , KBHNKZ = ?                        -- 品目課税区分
                            , KBKEIG = ?                        -- 軽減税率区分
                            , QTHCTN = ?                        -- 発注点数
                            , QTTZAI = ?                        -- 適正在庫数
                            , YMSIKO = ?                        -- 仕込開始年月
                            , QTHNEN = ?                        -- 平均算出年数
                            , RTKEPN = ?                        -- 欠品許容率
                            , CDSRIS = ?                        -- シリーズコード
                            , VLSIZE = ?                        -- サイズ
                            , CDDCKA = ?                        -- 債権貸方科目コード
                            , CDDDKA = ?                        -- 債権借方科目コード
                            , CDICKA = ?                        -- 債務(一般)貸方
                            , CDIDKA = ?                        -- 債務(一般)借方
                            , CDSCKA = ?                        -- 債務(製造)貸方
                            , CDSDKA = ?                        -- 債務(製造)借方
                            , PRSANK = ?                        -- 参考単価
                            , CDKASH = ?                        -- 加工担当部門
                            , ATKKOS = ?                        -- 加工工数
	            WHERE CDHINM = ?"; // 更新クエリ

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", Convert.ToDecimal(item.FGDELE));
                    command.Parameters.AddWithValue("@SPIUSR", string.IsNullOrEmpty(item.SPIUSR) ? (object)DBNull.Value : item.SPIUSR);
                    command.Parameters.AddWithValue("@SPIDTM", Convert.ToDecimal(item.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPIPGM", string.IsNullOrEmpty(item.SPIPGM) ? (object)DBNull.Value : item.SPIPGM);
                    command.Parameters.AddWithValue("@SPUUSR", string.IsNullOrEmpty(item.SPUUSR) ? (object)DBNull.Value : item.SPUUSR);
                    command.Parameters.AddWithValue("@SPUDTM", Convert.ToDecimal(item.SPUDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPUPGM", string.IsNullOrEmpty(item.SPUPGM) ? (object)DBNull.Value : item.SPUPGM);
                    command.Parameters.AddWithValue("@NMHINM", string.IsNullOrEmpty(item.NMHINM) ? (object)DBNull.Value : item.NMHINM);
                    command.Parameters.AddWithValue("@NRHINM", string.IsNullOrEmpty(item.NRHINM) ? (object)DBNull.Value : item.NRHINM);
                    command.Parameters.AddWithValue("@NKHINM", string.IsNullOrEmpty(item.NKHINM) ? (object)DBNull.Value : item.NKHINM);
                    command.Parameters.AddWithValue("@ENHINM", string.IsNullOrEmpty(item.ENHINM) ? (object)DBNull.Value : item.ENHINM);
                    command.Parameters.AddWithValue("@ERHINM", string.IsNullOrEmpty(item.ERHINM) ? (object)DBNull.Value : item.ERHINM);
                    command.Parameters.AddWithValue("@CDHINS", string.IsNullOrEmpty(item.CDHINS) ? (object)DBNull.Value : item.CDHINS);
                    command.Parameters.AddWithValue("@NMHISJ", string.IsNullOrEmpty(item.NMHISJ) ? (object)DBNull.Value : item.NMHISJ);
                    command.Parameters.AddWithValue("@NMHISE", string.IsNullOrEmpty(item.NMHISE) ? (object)DBNull.Value : item.NMHISE);
                    command.Parameters.AddWithValue("@CDTANI", string.IsNullOrEmpty(item.CDTANI) ? (object)DBNull.Value : item.CDTANI);
                    command.Parameters.AddWithValue("@KBUISI", string.IsNullOrEmpty(item.KBUISI) ? (object)DBNull.Value : item.KBUISI);
                    command.Parameters.AddWithValue("@KBCGHN", string.IsNullOrEmpty(item.KBCGHN) ? (object)DBNull.Value : item.KBCGHN);
                    command.Parameters.AddWithValue("@KBYOTO", string.IsNullOrEmpty(item.KBYOTO) ? (object)DBNull.Value : item.KBYOTO);
                    command.Parameters.AddWithValue("@KBKMTT", string.IsNullOrEmpty(item.KBKMTT) ? (object)DBNull.Value : item.KBKMTT);
                    command.Parameters.AddWithValue("@KBHNTY", string.IsNullOrEmpty(item.KBHNTY) ? (object)DBNull.Value : item.KBHNTY);
                    command.Parameters.AddWithValue("@CDHINB", string.IsNullOrEmpty(item.CDHINB) ? (object)DBNull.Value : item.CDHINB);
                    command.Parameters.AddWithValue("@CDHNGP", string.IsNullOrEmpty(item.CDHNGP) ? (object)DBNull.Value : item.CDHNGP);
                    command.Parameters.AddWithValue("@NOREVS", Convert.ToDecimal(item.NOREVS));
                    command.Parameters.AddWithValue("@KBSING", string.IsNullOrEmpty(item.KBSING) ? (object)DBNull.Value : item.KBSING);
                    command.Parameters.AddWithValue("@VLZUBN", string.IsNullOrEmpty(item.VLZUBN) ? (object)DBNull.Value : item.VLZUBN);
                    command.Parameters.AddWithValue("@CDHHIN", string.IsNullOrEmpty(item.CDHHIN) ? (object)DBNull.Value : item.CDHHIN);
                    command.Parameters.AddWithValue("@NMBCLS", string.IsNullOrEmpty(item.NMBCLS) ? (object)DBNull.Value : item.NMBCLS);
                    command.Parameters.AddWithValue("@TXZUMN", string.IsNullOrEmpty(item.TXZUMN) ? (object)DBNull.Value : item.TXZUMN);
                    command.Parameters.AddWithValue("@TXBRTX", string.IsNullOrEmpty(item.TXBRTX) ? (object)DBNull.Value : item.TXBRTX);
                    command.Parameters.AddWithValue("@KBHREN", string.IsNullOrEmpty(item.KBHREN) ? (object)DBNull.Value : item.KBHREN);
                    command.Parameters.AddWithValue("@NOSOZU", string.IsNullOrEmpty(item.NOSOZU) ? (object)DBNull.Value : item.NOSOZU);
                    command.Parameters.AddWithValue("@NOKIKN", string.IsNullOrEmpty(item.NOKIKN) ? (object)DBNull.Value : item.NOKIKN);
                    command.Parameters.AddWithValue("@TXZAIS", string.IsNullOrEmpty(item.TXZAIS) ? (object)DBNull.Value : item.TXZAIS);
                    command.Parameters.AddWithValue("@CDZAIS1", string.IsNullOrEmpty(item.CDZAIS1) ? (object)DBNull.Value : item.CDZAIS1);
                    command.Parameters.AddWithValue("@CDZAIS2", string.IsNullOrEmpty(item.CDZAIS2) ? (object)DBNull.Value : item.CDZAIS2);
                    command.Parameters.AddWithValue("@CDZAIS3", string.IsNullOrEmpty(item.CDZAIS3) ? (object)DBNull.Value : item.CDZAIS3);
                    command.Parameters.AddWithValue("@CDZAIR", string.IsNullOrEmpty(item.CDZAIR) ? (object)DBNull.Value : item.CDZAIR);
                    command.Parameters.AddWithValue("@CDGKAS", string.IsNullOrEmpty(item.CDGKAS) ? (object)DBNull.Value : item.CDGKAS);
                    command.Parameters.AddWithValue("@NOTANA", string.IsNullOrEmpty(item.NOTANA) ? (object)DBNull.Value : item.NOTANA);
                    command.Parameters.AddWithValue("@CDKISH", string.IsNullOrEmpty(item.CDKISH) ? (object)DBNull.Value : item.CDKISH);
                    command.Parameters.AddWithValue("@KBZAIR", string.IsNullOrEmpty(item.KBZAIR) ? (object)DBNull.Value : item.KBZAIR);
                    command.Parameters.AddWithValue("@KBZAIK", string.IsNullOrEmpty(item.KBZAIK) ? (object)DBNull.Value : item.KBZAIK);
                    command.Parameters.AddWithValue("@KBLTKR", string.IsNullOrEmpty(item.KBLTKR) ? (object)DBNull.Value : item.KBLTKR);
                    command.Parameters.AddWithValue("@KBSESK", string.IsNullOrEmpty(item.KBSESK) ? (object)DBNull.Value : item.KBSESK);
                    command.Parameters.AddWithValue("@FGMILL", string.IsNullOrEmpty(item.FGMILL) ? (object)DBNull.Value : item.FGMILL);
                    command.Parameters.AddWithValue("@FGKYOD", string.IsNullOrEmpty(item.FGKYOD) ? (object)DBNull.Value : item.FGKYOD);
                    command.Parameters.AddWithValue("@FGKENT", string.IsNullOrEmpty(item.FGKENT) ? (object)DBNull.Value : item.FGKENT);
                    command.Parameters.AddWithValue("@FGSIJI", string.IsNullOrEmpty(item.FGSIJI) ? (object)DBNull.Value : item.FGSIJI);
                    command.Parameters.AddWithValue("@FGZUMN", string.IsNullOrEmpty(item.FGZUMN) ? (object)DBNull.Value : item.FGZUMN);
                    command.Parameters.AddWithValue("@FGSHIY", string.IsNullOrEmpty(item.FGSHIY) ? (object)DBNull.Value : item.FGSHIY);
                    command.Parameters.AddWithValue("@FGNETU", string.IsNullOrEmpty(item.FGNETU) ? (object)DBNull.Value : item.FGNETU);
                    command.Parameters.AddWithValue("@FGSUNK", string.IsNullOrEmpty(item.FGSUNK) ? (object)DBNull.Value : item.FGSUNK);
                    command.Parameters.AddWithValue("@FGSETU", string.IsNullOrEmpty(item.FGSETU) ? (object)DBNull.Value : item.FGSETU);
                    command.Parameters.AddWithValue("@FGKHKG", string.IsNullOrEmpty(item.FGKHKG) ? (object)DBNull.Value : item.FGKHKG);
                    command.Parameters.AddWithValue("@FGKGSY", string.IsNullOrEmpty(item.FGKGSY) ? (object)DBNull.Value : item.FGKGSY);
                    command.Parameters.AddWithValue("@FGPKAN", string.IsNullOrEmpty(item.FGPKAN) ? (object)DBNull.Value : item.FGPKAN);
                    command.Parameters.AddWithValue("@FGZKEN", string.IsNullOrEmpty(item.FGZKEN) ? (object)DBNull.Value : item.FGZKEN);
                    command.Parameters.AddWithValue("@FGTANS", string.IsNullOrEmpty(item.FGTANS) ? (object)DBNull.Value : item.FGTANS);
                    command.Parameters.AddWithValue("@FGZTOS", string.IsNullOrEmpty(item.FGZTOS) ? (object)DBNull.Value : item.FGZTOS);
                    command.Parameters.AddWithValue("@FGZASK", string.IsNullOrEmpty(item.FGZASK) ? (object)DBNull.Value : item.FGZASK);
                    command.Parameters.AddWithValue("@FGTJIG", string.IsNullOrEmpty(item.FGTJIG) ? (object)DBNull.Value : item.FGTJIG);
                    command.Parameters.AddWithValue("@KBCYLI", string.IsNullOrEmpty(item.KBCYLI) ? (object)DBNull.Value : item.KBCYLI);
                    command.Parameters.AddWithValue("@KBGKAK", string.IsNullOrEmpty(item.KBGKAK) ? (object)DBNull.Value : item.KBGKAK);
                    command.Parameters.AddWithValue("@KBTEHI", item.KBTEHI ?? "999"); // デフォルトの値
                    command.Parameters.AddWithValue("@KBNYTS", string.IsNullOrEmpty(item.KBNYTS) ? (object)DBNull.Value : item.KBNYTS);
                    command.Parameters.AddWithValue("@KBNYKN", string.IsNullOrEmpty(item.KBNYKN) ? (object)DBNull.Value : item.KBNYKN);
                    command.Parameters.AddWithValue("@KBKNSA", string.IsNullOrEmpty(item.KBKNSA) ? (object)DBNull.Value : item.KBKNSA);
                    command.Parameters.AddWithValue("@QTLOTS", Convert.ToDecimal(item.QTLOTS)); // デフォルト値
                    command.Parameters.AddWithValue("@QTHCSU", Convert.ToDecimal(item.QTHCSU)); // デフォルト値
                    command.Parameters.AddWithValue("@QTIMOJ", Convert.ToDecimal(item.QTIMOJ)); // デフォルト値
                    command.Parameters.AddWithValue("@PRIMOT", Convert.ToDecimal(item.PRIMOT)); // デフォルト値
                    command.Parameters.AddWithValue("@VLBSUN", string.IsNullOrEmpty(item.VLBSUN) ? (object)DBNull.Value : item.VLBSUN);
                    command.Parameters.AddWithValue("@TXBSUN", string.IsNullOrEmpty(item.TXBSUN) ? (object)DBNull.Value : item.TXBSUN);
                    command.Parameters.AddWithValue("@VLSUN1", Convert.ToDecimal(item.VLSUN1)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN2", Convert.ToDecimal(item.VLSUN2)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN3", Convert.ToDecimal(item.VLSUN3)); // デフォルト値
                    command.Parameters.AddWithValue("@QTLDTM", Convert.ToDecimal(item.QTLDTM));
                    command.Parameters.AddWithValue("@TXLTTM", string.IsNullOrEmpty(item.TXLTTM) ? (object)DBNull.Value : item.TXLTTM);
                    command.Parameters.AddWithValue("@QTBUHJ", Convert.ToDecimal(item.QTBUHJ)); // デフォルト値
                    command.Parameters.AddWithValue("@TXBUHJ", string.IsNullOrEmpty(item.TXBUHJ) ? (object)DBNull.Value : item.TXBUHJ);
                    command.Parameters.AddWithValue("@QTTNJU", Convert.ToDecimal(item.QTTNJU)); // デフォルト値
                    command.Parameters.AddWithValue("@CDDFSH", string.IsNullOrEmpty(item.CDDFSH) ? (object)DBNull.Value : item.CDDFSH);
                    command.Parameters.AddWithValue("@CDMEKA", string.IsNullOrEmpty(item.CDMEKA) ? (object)DBNull.Value : item.CDMEKA);
                    command.Parameters.AddWithValue("@CDDAIR", string.IsNullOrEmpty(item.CDDAIR) ? (object)DBNull.Value : item.CDDAIR);
                    command.Parameters.AddWithValue("@TXMKAT", string.IsNullOrEmpty(item.TXMKAT) ? (object)DBNull.Value : item.TXMKAT);
                    command.Parameters.AddWithValue("@TXMEKA", string.IsNullOrEmpty(item.TXMEKA) ? (object)DBNull.Value : item.TXMEKA);
                    command.Parameters.AddWithValue("@TXBUHN", string.IsNullOrEmpty(item.TXBUHN) ? (object)DBNull.Value : item.TXBUHN);
                    command.Parameters.AddWithValue("@TXB512", string.IsNullOrEmpty(item.TXB512) ? (object)DBNull.Value : item.TXB512);
                    command.Parameters.AddWithValue("@CDSOKO", string.IsNullOrEmpty(item.CDSOKO) ? (object)DBNull.Value : item.CDSOKO);
                    command.Parameters.AddWithValue("@KBHNKZ", string.IsNullOrEmpty(item.KBHNKZ) ? (object)DBNull.Value : item.KBHNKZ);
                    command.Parameters.AddWithValue("@KBKEIG", string.IsNullOrEmpty(item.KBKEIG) ? (object)DBNull.Value : item.KBKEIG);
                    command.Parameters.AddWithValue("@QTHCTN", Convert.ToDecimal(item.QTHCTN)); // デフォルト値
                    command.Parameters.AddWithValue("@QTTZAI", Convert.ToDecimal(item.QTTZAI)); // デフォルト値
                    command.Parameters.AddWithValue("@YMSIKO", Convert.ToDecimal(item.YMSIKO));
                    command.Parameters.AddWithValue("@QTHNEN", Convert.ToDecimal(item.QTHNEN)); // デフォルト値
                    command.Parameters.AddWithValue("@RTKEPN", Convert.ToDecimal(item.RTKEPN)); // デフォルト値
                    command.Parameters.AddWithValue("@CDSRIS", string.IsNullOrEmpty(item.CDSRIS) ? (object)DBNull.Value : item.CDSRIS);
                    command.Parameters.AddWithValue("@VLSIZE", Convert.ToDecimal(item.VLSIZE));
                    command.Parameters.AddWithValue("@CDDCKA", string.IsNullOrEmpty(item.CDDCKA) ? (object)DBNull.Value : item.CDDCKA);
                    command.Parameters.AddWithValue("@CDDDKA", string.IsNullOrEmpty(item.CDDDKA) ? (object)DBNull.Value : item.CDDDKA);
                    command.Parameters.AddWithValue("@CDICKA", string.IsNullOrEmpty(item.CDICKA) ? (object)DBNull.Value : item.CDICKA);
                    command.Parameters.AddWithValue("@CDIDKA", string.IsNullOrEmpty(item.CDIDKA) ? (object)DBNull.Value : item.CDIDKA);
                    command.Parameters.AddWithValue("@CDSCKA", string.IsNullOrEmpty(item.CDSCKA) ? (object)DBNull.Value : item.CDSCKA);
                    command.Parameters.AddWithValue("@CDSDKA", string.IsNullOrEmpty(item.CDSDKA) ? (object)DBNull.Value : item.CDSDKA);
                    command.Parameters.AddWithValue("@PRSANK", Convert.ToDecimal(item.PRSANK)); // デフォルト値
                    command.Parameters.AddWithValue("@CDKASH", string.IsNullOrEmpty(item.CDKASH) ? (object)DBNull.Value : item.CDKASH);
                    command.Parameters.AddWithValue("@ATKKOS", Convert.ToDecimal(item.ATKKOS)); // デフォルト値
                    command.Parameters.AddWithValue("@CDHINM", item.CDHINM); // 条件用の品目コードを追加
                    command.Transaction = this.transaction;
                    command.ExecuteNonQuery(); // 更新を実行
                }
            }
        }
        //
        public class ItemMasterHistoryRepository
        { 
            // *********************************************************************************
            // * 品目マスタ履歴ファイルのエンティティクラス
            // *********************************************************************************
            private OdbcConnection _connection;
            private OdbcTransaction transaction;

            // コンストラクタ
            public ItemMasterHistoryRepository(OdbcConnection connection, OdbcTransaction transaction)
            {
                _connection = connection;
                this.transaction = transaction;
            }
            // 
            public class ItemMasterHistory
            {
                public decimal FGDELE { get; set; }					                                                                                                //削除フラグ
                public string SPIUSR { get; set; }                                                                                                                  //登録担当者コード
                public decimal SPIDTM { get; set; }                                                                                                                 //登録日時
                public string SPIPGM { get; set; }                                                                                                                  //登録プログラムID
                public string SPUUSR { get; set; }                                                                                                                  //更新担当者コード
                public decimal SPUDTM { get; set; }                                                                                                                 //更新日時
                public string SPUPGM { get; set; }                                                                                                                  //更新プログラムID
                public decimal HistoryDate { get; set; }                                                                                                            //  履歴日
                public decimal HistoryTime { get; set; }                                                                                                            //  履歴時刻
                public string ItemCode { get; set; }                                                                                                                //  品目コード
                public string ItemName { get; set; } = string.Empty;                                                                                                //  品目名
                public string ItemShortName { get; set; } = string.Empty;                                                                                           //  品目名略称
                public string ItemKanaName { get; set; } = string.Empty;                                                                                            //  品目カナ名
                public string ItemEnglishName { get; set; } = string.Empty;                                                                                         //  品目英名
                public string ItemEnglishShortName { get; set; } = string.Empty;                                                                                    //  品目英名略称
                public string VarietyCode { get; set; } = string.Empty;                                                                                             //  品種コード
                public string VarietyNameJapanese { get; set; } = string.Empty;                                                                                     //  品種名称和文
                public string VarietyNameEnglish { get; set; } = string.Empty;                                                                                      //  品種名称英文
                public string UnitCode { get; set; } = string.Empty;                                                                                                //  単位コード
                public string SalesPurchaseUsage { get; set; } = string.Empty;                                                                                      //  売上仕入使用区分
                public string ItemNameChangeFlag { get; set; } = string.Empty;                                                                                      //  品目名称変更区分
                public string PurposeCode { get; set; } = string.Empty;                                                                                             //  用途区分
                public string AssemblyCode { get; set; } = string.Empty;                                                                                            //  組立区分
                public string ItemTypeCode { get; set; } = string.Empty;                                                                                            //  品目タイプ区分
                public string ItemClassificationCode { get; set; } = string.Empty;                                                                                  //  品目分類コード
                public string ItemGroupCode { get; set; } = string.Empty;                                                                                           //  品目グループコード
                public decimal RevisionNumber { get; set; } = 0;                                                                                                    //  改訂番号
                public string UnavailableFlag { get; set; } = string.Empty;                                                                                         //  使用不可区分
                public string DrawingNumber { get; set; } = string.Empty;                                                                                           //  図番/購入仕様書番号
                public string ChangedItemCode { get; set; } = string.Empty;                                                                                         //  変更後品目コード
                public string BaseRightClassName { get; set; } = string.Empty;                                                                                      //  Base-Rightクラス名
                public string DrawingRemarks { get; set; } = string.Empty;                                                                                          //  図面番号特記事項
                public string BRRemarks { get; set; } = string.Empty;                                                                                               //  BR特記事項
                public string ItemNumberLinkFlag { get; set; } = string.Empty;                                                                                      //  品番連携区分
                public string MaterialDrawing { get; set; } = string.Empty;                                                                                         //  素材図
                public string WoodPatternMetalPattern { get; set; } = string.Empty;                                                                                 //  木型・金型
                public string Material { get; set; } = string.Empty;                                                                                                //  材質
                public string MaterialCode1 { get; set; } = string.Empty;                                                                                           //  材質コード1
                public string MaterialCode2 { get; set; } = string.Empty;                                                                                           //  材質コード2
                public string MaterialCode3 { get; set; } = string.Empty;                                                                                           //  材質コード3
                public string MaterialItemCode { get; set; } = string.Empty;                                                                                        //  材料品目コード
                public string OutsourcingProcessCode { get; set; } = string.Empty;                                                                                  //  外注加工先
                public string ShelfCode { get; set; } = string.Empty;                                                                                               //  棚番
                public string RepresentativeModelCode { get; set; } = string.Empty;                                                                                 //  代表型式コード
                public string MaterialClassificationCode { get; set; } = string.Empty;                                                                              //  材料区分
                public string StockClassificationCode { get; set; } = string.Empty;                                                                                 //  在庫区分
                public string LotManagementFlag { get; set; } = string.Empty;                                                                                       //  ロット管理区分
                public string ResultRecordHandlingFlag { get; set; } = string.Empty;                                                                                //  成績書扱い区分
                public string MillSheetRequiredFlag { get; set; } = string.Empty;                                                                                   //  ミルシート要否
                public string StrengthCalculationRequiredFlag { get; set; } = string.Empty;                                                                         //  強度計算書有無
                public string InspectionRequiredFlag { get; set; } = string.Empty;                                                                                  //  検定要否
                public string InstructionRequiredFlag { get; set; } = string.Empty;                                                                                 //  指示書要否
                public string DrawingRequiredFlag { get; set; } = string.Empty;                                                                                     //  図面要否
                public string SpecificationRequiredFlag { get; set; } = string.Empty;                                                                               //  仕様書要否
                public string HeatTreatmentRecordRequiredFlag { get; set; } = string.Empty;                                                                         //  熱処理記録要否
                public string DimensionConfirmationRequiredFlag { get; set; } = string.Empty;                                                                       //  寸法確認書要否
                public string InstructionManualRequiredFlag { get; set; } = string.Empty;                                                                           //  取扱説明書要否
                public string KHKCertificateRequiredFlag { get; set; } = string.Empty;                                                                              //  KHK合格書要否
                public string HighPressureGasCertificationRequiredFlag { get; set; } = string.Empty;                                                                //  高圧ガス認定書要否
                public string ProcessManagementCode { get; set; } = string.Empty;                                                                                   //  プロセス管理区分
                public string MaterialInspectionRequiredFlag { get; set; } = string.Empty;                                                                          //  材料検査有無
                public string SingleShippingFlag { get; set; } = string.Empty;                                                                                      //  単体出荷可否
                public string MaterialCoatingRequiredFlag { get; set; } = string.Empty;                                                                             //  材料時塗装要否
                public string MaterialProvisionRequiredFlag { get; set; } = string.Empty;                                                                           //  材料支給要否
                public string PressureTestingRequiredFlag { get; set; } = string.Empty;                                                                             //  耐圧治具有無
                public string OldNewCylinderClassification { get; set; } = string.Empty;                                                                            //  新旧シリンダ区分
                public string ProcessingClassification { get; set; } = string.Empty;                                                                                //  加工区分
                public string ArrangementClassification { get; set; } = string.Empty;                                                                               //  手配区分
                public string InboundManagementApplicableFlag { get; set; } = string.Empty;                                                                         //  入荷管理対象区分
                public string InboundInspectionRequiredFlag { get; set; } = string.Empty;                                                                           //  入荷検査要否区分
                public string InspectionClassification { get; set; } = string.Empty;                                                                                //  検査区分
                public decimal LotQuantity { get; set; } = 0;                                                                                                       //  ロット数量
                public decimal MinimumOrderQuantity { get; set; } = 0;                                                                                              //  最小発注数量
                public decimal CastingWeight { get; set; } = 0;                                                                                                     //  鋳物単重
                public decimal CastingCost { get; set; } = 0;                                                                                                       //  鋳物原価
                public string ComponentSizeLWH { get; set; } = string.Empty;                                                                                        //  部品寸法LWH(mm)
                public string ComponentSizeRemarks { get; set; } = string.Empty;                                                                                    //  部品寸法特記事項
                public decimal Dimension1 { get; set; } = 0;                                                                                                        //  寸法1
                public decimal Dimension2 { get; set; } = 0;                                                                                                        //  寸法2
                public decimal Dimension3 { get; set; } = 0;                                                                                                        //  寸法3
                public decimal ItemLT { get; set; } = 0;                                                                                                            //  品目LT
                public string ItemLTRemarks { get; set; } = string.Empty;                                                                                           //  品目LT特記事項
                public decimal ComponentSingleWeight { get; set; } = 0;                                                                                             //  部品単体重量(kg)
                public string ComponentSingleWeightRemarks { get; set; } = string.Empty;                                                                            //  部品単体重量特記事項
                public decimal UnitWeight { get; set; } = 0;                                                                                                        //  単重
                public string StandardSupplierCode { get; set; } = string.Empty;                                                                                    //  標準仕入先コード
                public string ManufacturerCode { get; set; } = string.Empty;                                                                                        //  メーカーコード
                public string StandardAgentCode { get; set; } = string.Empty;                                                                                       //  標準代理店コード
                public string ManufacturerModelNumber { get; set; } = string.Empty;                                                                                 //  メーカー型番
                public string ManufacturerRemarks { get; set; } = string.Empty;                                                                                     //  メーカー特記事項
                public string ComponentDetail { get; set; } = string.Empty;                                                                                         //  部品詳細内容
                public string Remarks { get; set; } = string.Empty;                                                                                                 //  特記事項
                public string WarehouseCode { get; set; } = string.Empty;                                                                                           //  倉庫コード
                public string ItemTaxClassification { get; set; } = string.Empty;                                                                                   //  品目課税区分
                public string ReducedTaxRateClassification { get; set; } = string.Empty;                                                                            //  軽減税率区分
                public decimal OrderPointQuantity { get; set; } = 0;                                                                                                //  発注点数
                public decimal AppropriateStockQuantity { get; set; } = 0;                                                                                          //  適正在庫数
                public decimal StartDateForPreparation { get; set; } = 0;                                                                                           //  仕込開始年月
                public decimal AverageCalculationYears { get; set; } = 0;                                                                                           //  平均算出年数
                public decimal StockOutToleranceRate { get; set; } = 0;                                                                                             //  欠品許容率
                public string SeriesCode { get; set; } = string.Empty;                                                                                                //  シリーズコード
                public decimal Size { get; set; } = 0;                                                                                                              //  サイズ
                public string ReceivableCreditAccountCode { get; set; } = string.Empty;                                                                             //  債権貸方科目コード
                public string PayableDebitAccountCode { get; set; } = string.Empty;                                                                                 //  債権借方科目コード
                public string LiabilityGeneralCreditAccountCode { get; set; } = string.Empty;                                                                       //  債務(一般)貸方科目コード
                public string LiabilityGeneralDebitAccountCode { get; set; } = string.Empty;                                                                        //  債務(一般)借方科目コード
                public string LiabilityManufacturingCreditAccountCode { get; set; } = string.Empty;                                                                 //  債務(製造)貸方科目コード
                public string LiabilityManufacturingDebitAccountCode { get; set; } = string.Empty;                                                                  //  債務(製造)借方科目コード
                public decimal ReferencePrice { get; set; } = 0;                                                                                                    //  参考単価
                public string ProcessingDepartmentCode { get; set; } = string.Empty;                                                                                //  加工担当部門
                public decimal ProcessingManHours { get; set; } = 0;                                                                                                //  加工工数
            }
            public void Insert(OdbcConnection connection , ItemMasterHistory ItemMasterHistory)
            {
                string sql = @"INSERT 
INTO FHINM( 
    FGDELE                                      -- 削除フラグ
    , SPIUSR                                    -- 登録担当者コード
    , SPIDTM                                    -- 登録日時
    , SPIPGM                                    -- 登録プログラムID
    , SPUUSR                                    -- 更新担当者コード
    , SPUDTM                                    -- 更新日時
    , SPUPGM                                    -- 更新プログラムID
    , DTRREK                                    -- 履歴日
    , TMRREK                                    -- 履歴時刻
    , CDHINM                                    -- 品目コード
    , NMHINM                                    -- 品目名
    , NRHINM                                    -- 品目名略称
    , NKHINM                                    -- 品目カナ名
    , ENHINM                                    -- 品目英名
    , ERHINM                                    -- 品目英名略称
    , CDHINS                                    -- 品種コード
    , NMHISJ                                    -- 品種名称和文
    , NMHISE                                    -- 品種名称英文
    , CDTANI                                    -- 単位コード
    , KBUISI                                    -- 売上仕入使用区分
    , KBCGHN                                    -- 品目名称変更区分
    , KBYOTO                                    -- 用途区分
    , KBKMTT                                    -- 組立区分
    , KBHNTY                                    -- 品目タイプ区分
    , CDHINB                                    -- 品目分類コード
    , CDHNGP                                    -- 品目グループコード
    , NOREVS                                    -- 改訂番号
    , KBSING                                    -- 使用不可区分
    , VLZUBN                                    -- 図番/購入仕様書
    , CDHHIN                                    -- 変更後品目コード
    , NMBCLS                                    -- Base-Rightクラス
    , TXZUMN                                    -- 図面番号特記事項
    , TXBRTX                                    -- BR特記事項
    , KBHREN                                    -- 品番連携区分
    , NOSOZU                                    -- 素材図
    , NOKIKN                                    -- 木型・金型
    , TXZAIS                                    -- 材質
    , CDZAIS1                                   -- 材質コード1
    , CDZAIS2                                   -- 材質コード2
    , CDZAIS3                                   -- 材質コード3
    , CDZAIR                                    -- 材料品目コード
    , CDGKAS                                    -- 外注加工先
    , NOTANA                                    -- 棚番
    , CDKISH                                    -- 代表型式コード
    , KBZAIR                                    -- 材料区分
    , KBZAIK                                    -- 在庫区分
    , KBLTKR                                    -- ロット管理区分
    , KBSESK                                    -- 成績書扱い区分
    , FGMILL                                    -- ミルシート要否
    , FGKYOD                                    -- 強度計算書有無
    , FGKENT                                    -- 検定要否
    , FGSIJI                                    -- 指示書要否
    , FGZUMN                                    -- 図面要否
    , FGSHIY                                    -- 仕様書要否
    , FGNETU                                    -- 熱処理記録要否
    , FGSUNK                                    -- 寸法確認書要否
    , FGSETU                                    -- 取扱説明書要否
    , FGKHKG                                    -- KHK合格書要否
    , FGKGSY                                    -- 高圧ガス認定書要否
    , FGPKAN                                    -- プロセス管理区分
    , FGZKEN                                    -- 材料検査有無
    , FGTANS                                    -- 単体出荷可否
    , FGZTOS                                    -- 材料時塗装要否
    , FGZASK                                    -- 材料支給要否
    , FGTJIG                                    -- 耐圧治具有無
    , KBCYLI                                    -- 新旧シリンダ区分
    , KBGKAK                                    -- 加工区分
    , KBTEHI                                    -- 手配区分
    , KBNYTS                                    -- 入荷管理対象区分
    , KBNYKN                                    -- 入荷検査要否区分
    , KBKNSA                                    -- 検査区分
    , QTLOTS                                    -- ロット数量
    , QTHCSU                                    -- 最小発注数量
    , QTIMOJ                                    -- 鋳物単重
    , PRIMOT                                    -- 鋳物原価
    , VLBSUN                                    -- 部品寸法LWH(mm)
    , TXBSUN                                    -- 部品寸法特記事項
    , VLSUN1                                    -- 寸法1
    , VLSUN2                                    -- 寸法2
    , VLSUN3                                    -- 寸法3
    , QTLDTM                                    -- 品目LT
    , TXLTTM                                    -- 品目LT特記事項
    , QTBUHJ                                    -- 部品単体重量(kg)
    , TXBUHJ                                    -- 部品単体重量特記事
    , QTTNJU                                    -- 単重
    , CDDFSH                                    -- 標準仕入先コード
    , CDMEKA                                    -- メーカーコード
    , CDDAIR                                    -- 標準代理店コード
    , TXMKAT                                    -- メーカー型番
    , TXMEKA                                    -- メーカー特記事項
    , TXBUHN                                    -- 部品詳細内容
    , TXB512                                    -- 特記事項
    , CDSOKO                                    -- 倉庫コード
    , KBHNKZ                                    -- 品目課税区分
    , KBKEIG                                    -- 軽減税率区分
    , QTHCTN                                    -- 発注点数
    , QTTZAI                                    -- 適正在庫数
    , YMSIKO                                    -- 仕込開始年月
    , QTHNEN                                    -- 平均算出年数
    , RTKEPN                                    -- 欠品許容率
    , CDSRIS                                    -- シリーズコード
    , VLSIZE                                    -- サイズ
    , CDDCKA                                    -- 債権貸方科目コード
    , CDDDKA                                    -- 債権借方科目コード
    , CDICKA                                    -- 債務(一般)貸方
    , CDIDKA                                    -- 債務(一般)借方
    , CDSCKA                                    -- 債務(製造)貸方
    , CDSDKA                                    -- 債務(製造)借方
    , PRSANK                                    -- 参考単価
    , CDKASH                                    -- 加工担当部門
    , ATKKOS                                    -- 加工工数
) 
VALUES ( 
      ?                                  -- 削除フラグ
    , ?                                  -- 登録担当者コード
    , ?                                  -- 登録日時
    , ?                                  -- 登録プログラムID
    , ?                                  -- 更新担当者コード
    , ?                                  -- 更新日時
    , ?                                  -- 更新プログラムID
    , ?                                  -- 履歴日
    , ?                                  -- 履歴時刻
    , ?                                  -- 品目コード
    , ?                                  -- 品目名
    , ?                                  -- 品目名略称
    , ?                                  -- 品目カナ名
    , ?                                  -- 品目英名
    , ?                                  -- 品目英名略称
    , ?                                  -- 品種コード
    , ?                                  -- 品種名称和文
    , ?                                  -- 品種名称英文
    , ?                                  -- 単位コード
    , ?                                  -- 売上仕入使用区分
    , ?                                  -- 品目名称変更区分
    , ?                                  -- 用途区分
    , ?                                  -- 組立区分
    , ?                                  -- 品目タイプ区分
    , ?                                  -- 品目分類コード
    , ?                                  -- 品目グループコード
    , ?                                  -- 改訂番号
    , ?                                  -- 使用不可区分
    , ?                                  -- 図番/購入仕様書
    , ?                                  -- 変更後品目コード
    , ?                                  -- Base-Rightクラス
    , ?                                  -- 図面番号特記事項
    , ?                                  -- BR特記事項
    , ?                                  -- 品番連携区分
    , ?                                  -- 素材図
    , ?                                  -- 木型・金型
    , ?                                  -- 材質
    , ?                                  -- 材質コード1
    , ?                                  -- 材質コード2
    , ?                                  -- 材質コード3
    , ?                                  -- 材料品目コード
    , ?                                  -- 外注加工先
    , ?                                  -- 棚番
    , ?                                  -- 代表型式コード
    , ?                                  -- 材料区分
    , ?                                  -- 在庫区分
    , ?                                  -- ロット管理区分
    , ?                                  -- 成績書扱い区分
    , ?                                  -- ミルシート要否
    , ?                                  -- 強度計算書有無
    , ?                                  -- 検定要否
    , ?                                  -- 指示書要否
    , ?                                  -- 図面要否
    , ?                                  -- 仕様書要否
    , ?                                  -- 熱処理記録要否
    , ?                                  -- 寸法確認書要否
    , ?                                  -- 取扱説明書要否
    , ?                                  -- KHK合格書要否
    , ?                                  -- 高圧ガス認定書要否
    , ?                                  -- プロセス管理区分
    , ?                                  -- 材料検査有無
    , ?                                  -- 単体出荷可否
    , ?                                  -- 材料時塗装要否
    , ?                                  -- 材料支給要否
    , ?                                  -- 耐圧治具有無
    , ?                                  -- 新旧シリンダ区分
    , ?                                  -- 加工区分
    , ?                                  -- 手配区分
    , ?                                  -- 入荷管理対象区分
    , ?                                  -- 入荷検査要否区分
    , ?                                  -- 検査区分
    , ?                                  -- ロット数量
    , ?                                  -- 最小発注数量
    , ?                                  -- 鋳物単重
    , ?                                  -- 鋳物原価
    , ?                                  -- 部品寸法LWH(mm)
    , ?                                  -- 部品寸法特記事項
    , ?                                  -- 寸法1
    , ?                                  -- 寸法2
    , ?                                  -- 寸法3
    , ?                                  -- 品目LT
    , ?                                  -- 品目LT特記事項
    , ?                                  -- 部品単体重量(kg)
    , ?                                  -- 部品単体重量特記事
    , ?                                  -- 単重
    , ?                                  -- 標準仕入先コード
    , ?                                  -- メーカーコード
    , ?                                  -- 標準代理店コード
    , ?                                  -- メーカー型番
    , ?                                  -- メーカー特記事項
    , ?                                  -- 部品詳細内容
    , ?                                  -- 特記事項
    , ?                                  -- 倉庫コード
    , ?                                  -- 品目課税区分
    , ?                                  -- 軽減税率区分
    , ?                                  -- 発注点数
    , ?                                  -- 適正在庫数
    , ?                                  -- 仕込開始年月
    , ?                                  -- 平均算出年数
    , ?                                  -- 欠品許容率
    , ?                                  -- シリーズコード
    , ?                                  -- サイズ
    , ?                                  -- 債権貸方科目コード
    , ?                                  -- 債権借方科目コード
    , ?                                  -- 債務(一般)貸方
    , ?                                  -- 債務(一般)借方
    , ?                                  -- 債務(製造)貸方
    , ?                                  -- 債務(製造)借方
    , ?                                  -- 参考単価
    , ?                                  -- 加工担当部門
    , ?                                  -- 加工工数
)
"; 

                using (var command = new OdbcCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", Convert.ToDecimal(ItemMasterHistory.FGDELE));
                    command.Parameters.AddWithValue("@SPIUSR", string.IsNullOrEmpty(ItemMasterHistory.SPIUSR) ? (object)DBNull.Value : ItemMasterHistory.SPIUSR);
                    command.Parameters.AddWithValue("@SPIDTM", Convert.ToDecimal(ItemMasterHistory.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPIPGM", string.IsNullOrEmpty(ItemMasterHistory.SPIPGM) ? (object)DBNull.Value : ItemMasterHistory.SPIPGM);
                    command.Parameters.AddWithValue("@SPUUSR", string.IsNullOrEmpty(ItemMasterHistory.SPUUSR) ? (object)DBNull.Value : ItemMasterHistory.SPUUSR);
                    command.Parameters.AddWithValue("@SPUDTM", Convert.ToDecimal(ItemMasterHistory.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPUPGM", string.IsNullOrEmpty(ItemMasterHistory.SPUPGM) ? (object)DBNull.Value : ItemMasterHistory.SPUPGM);
                    command.Parameters.AddWithValue("@DTRREK", Convert.ToDecimal(ItemMasterHistory.HistoryDate));                                                                                                                       //履歴日                
                    command.Parameters.AddWithValue("@TMRREK", Convert.ToDecimal(ItemMasterHistory.HistoryTime));                                                                                                                       //履歴時刻
                    command.Parameters.AddWithValue("@CDHINM", string.IsNullOrEmpty(ItemMasterHistory.ItemCode) ? (object)DBNull.Value : ItemMasterHistory.ItemCode);                                                                   //品目コード
                    command.Parameters.AddWithValue("@NMHINM", string.IsNullOrEmpty(ItemMasterHistory.ItemName) ? (object)DBNull.Value : ItemMasterHistory.ItemName);                                                                   //品目名
                    command.Parameters.AddWithValue("@NRHINM", string.IsNullOrEmpty(ItemMasterHistory.ItemShortName) ? (object)DBNull.Value : ItemMasterHistory.ItemShortName);                                                         //品目名略称
                    command.Parameters.AddWithValue("@NKHINM", string.IsNullOrEmpty(ItemMasterHistory.ItemKanaName) ? (object)DBNull.Value : ItemMasterHistory.ItemKanaName);                                                           //品目カナ名
                    command.Parameters.AddWithValue("@ENHINM", string.IsNullOrEmpty(ItemMasterHistory.ItemEnglishName) ? (object)DBNull.Value : ItemMasterHistory.ItemEnglishName);                                                     //品目英名
                    command.Parameters.AddWithValue("@ERHINM", string.IsNullOrEmpty(ItemMasterHistory.ItemEnglishShortName) ? (object)DBNull.Value : ItemMasterHistory.ItemEnglishShortName);                                           //品目英名略称
                    command.Parameters.AddWithValue("@CDHINS", string.IsNullOrEmpty(ItemMasterHistory.VarietyCode) ? (object)DBNull.Value : ItemMasterHistory.VarietyCode);                                                             //品種コード
                    command.Parameters.AddWithValue("@NMHISJ", string.IsNullOrEmpty(ItemMasterHistory.VarietyNameJapanese) ? (object)DBNull.Value : ItemMasterHistory.VarietyNameJapanese);                                             //品種名称和文
                    command.Parameters.AddWithValue("@NMHISE", string.IsNullOrEmpty(ItemMasterHistory.VarietyNameEnglish) ? (object)DBNull.Value : ItemMasterHistory.VarietyNameEnglish);                                               //品種名称英文
                    command.Parameters.AddWithValue("@CDTANI", string.IsNullOrEmpty(ItemMasterHistory.UnitCode) ? (object)DBNull.Value : ItemMasterHistory.UnitCode);                                                                   //単位コード
                    command.Parameters.AddWithValue("@KBUISI", string.IsNullOrEmpty(ItemMasterHistory.SalesPurchaseUsage) ? (object)DBNull.Value : ItemMasterHistory.SalesPurchaseUsage);                                               //売上仕入使用区分
                    command.Parameters.AddWithValue("@KBCGHN", string.IsNullOrEmpty(ItemMasterHistory.ItemNameChangeFlag) ? (object)DBNull.Value : ItemMasterHistory.ItemNameChangeFlag);                                               //品目名称変更区分
                    command.Parameters.AddWithValue("@KBYOTO", string.IsNullOrEmpty(ItemMasterHistory.PurposeCode) ? (object)DBNull.Value : ItemMasterHistory.PurposeCode);                                                             //用途区分
                    command.Parameters.AddWithValue("@KBKMTT", string.IsNullOrEmpty(ItemMasterHistory.AssemblyCode) ? (object)DBNull.Value : ItemMasterHistory.AssemblyCode);                                                           //組立区分
                    command.Parameters.AddWithValue("@KBHNTY", string.IsNullOrEmpty(ItemMasterHistory.ItemTypeCode) ? (object)DBNull.Value : ItemMasterHistory.ItemTypeCode);                                                           //品目タイプ区分
                    command.Parameters.AddWithValue("@CDHINB", string.IsNullOrEmpty(ItemMasterHistory.ItemClassificationCode) ? (object)DBNull.Value : ItemMasterHistory.ItemClassificationCode);                                       //品目分類コード
                    command.Parameters.AddWithValue("@CDHNGP", string.IsNullOrEmpty(ItemMasterHistory.ItemGroupCode) ? (object)DBNull.Value : ItemMasterHistory.ItemGroupCode);                                                         //品目グループコード
                    command.Parameters.AddWithValue("@NOREVS", Convert.ToDecimal(ItemMasterHistory.RevisionNumber));                                                                                                                    //改訂番号
                    command.Parameters.AddWithValue("@KBSING", string.IsNullOrEmpty(ItemMasterHistory.UnavailableFlag) ? (object)DBNull.Value : ItemMasterHistory.UnavailableFlag);                                                     //使用不可区分
                    command.Parameters.AddWithValue("@VLZUBN", string.IsNullOrEmpty(ItemMasterHistory.DrawingNumber) ? (object)DBNull.Value : ItemMasterHistory.DrawingNumber);                                                         //図番/購入仕様書番号
                    command.Parameters.AddWithValue("@CDHHIN", string.IsNullOrEmpty(ItemMasterHistory.ChangedItemCode) ? (object)DBNull.Value : ItemMasterHistory.ChangedItemCode);                                                     //変更後品目コード
                    command.Parameters.AddWithValue("@NMBCLS", string.IsNullOrEmpty(ItemMasterHistory.BaseRightClassName) ? (object)DBNull.Value : ItemMasterHistory.BaseRightClassName);                                               //Base-Rightクラス名
                    command.Parameters.AddWithValue("@TXZUMN", string.IsNullOrEmpty(ItemMasterHistory.DrawingRemarks) ? (object)DBNull.Value : ItemMasterHistory.DrawingRemarks);                                                       //図面番号特記事項
                    command.Parameters.AddWithValue("@TXBRTX", string.IsNullOrEmpty(ItemMasterHistory.BRRemarks) ? (object)DBNull.Value : ItemMasterHistory.BRRemarks);                                                                 //BR特記事項
                    command.Parameters.AddWithValue("@KBHREN", string.IsNullOrEmpty(ItemMasterHistory.ItemNumberLinkFlag) ? (object)DBNull.Value : ItemMasterHistory.ItemNumberLinkFlag);                                               //品番連携区分
                    command.Parameters.AddWithValue("@NOSOZU", string.IsNullOrEmpty(ItemMasterHistory.MaterialDrawing) ? (object)DBNull.Value : ItemMasterHistory.MaterialDrawing);                                                     //素材図
                    command.Parameters.AddWithValue("@NOKIKN", string.IsNullOrEmpty(ItemMasterHistory.WoodPatternMetalPattern) ? (object)DBNull.Value : ItemMasterHistory.WoodPatternMetalPattern);                                     //木型・金型
                    command.Parameters.AddWithValue("@TXZAIS", string.IsNullOrEmpty(ItemMasterHistory.Material) ? (object)DBNull.Value : ItemMasterHistory.Material);                                                                   //材質
                    command.Parameters.AddWithValue("@CDZAIS1", string.IsNullOrEmpty(ItemMasterHistory.MaterialCode1) ? (object)DBNull.Value : ItemMasterHistory.MaterialCode1);                                                        //材質コード1
                    command.Parameters.AddWithValue("@CDZAIS2", string.IsNullOrEmpty(ItemMasterHistory.MaterialCode2) ? (object)DBNull.Value : ItemMasterHistory.MaterialCode2);                                                        //材質コード2
                    command.Parameters.AddWithValue("@CDZAIS3", string.IsNullOrEmpty(ItemMasterHistory.MaterialCode3) ? (object)DBNull.Value : ItemMasterHistory.MaterialCode3);                                                        //材質コード3
                    command.Parameters.AddWithValue("@CDZAIR", string.IsNullOrEmpty(ItemMasterHistory.MaterialItemCode) ? (object)DBNull.Value : ItemMasterHistory.MaterialItemCode);                                                   //材料品目コード
                    command.Parameters.AddWithValue("@CDGKAS", string.IsNullOrEmpty(ItemMasterHistory.OutsourcingProcessCode) ? (object)DBNull.Value : ItemMasterHistory.OutsourcingProcessCode);                                       //外注加工先
                    command.Parameters.AddWithValue("@NOTANA", string.IsNullOrEmpty(ItemMasterHistory.ShelfCode) ? (object)DBNull.Value : ItemMasterHistory.ShelfCode);                                                                 //棚番
                    command.Parameters.AddWithValue("@CDKISH", string.IsNullOrEmpty(ItemMasterHistory.RepresentativeModelCode) ? (object)DBNull.Value : ItemMasterHistory.RepresentativeModelCode);                                     //代表型式コード
                    command.Parameters.AddWithValue("@KBZAIR", string.IsNullOrEmpty(ItemMasterHistory.MaterialClassificationCode) ? (object)DBNull.Value : ItemMasterHistory.MaterialClassificationCode);                               //材料区分
                    command.Parameters.AddWithValue("@KBZAIK", string.IsNullOrEmpty(ItemMasterHistory.StockClassificationCode) ? (object)DBNull.Value : ItemMasterHistory.StockClassificationCode);                                     //在庫区分
                    command.Parameters.AddWithValue("@KBLTKR", string.IsNullOrEmpty(ItemMasterHistory.LotManagementFlag) ? (object)DBNull.Value : ItemMasterHistory.LotManagementFlag);                                                 //ロット管理区分
                    command.Parameters.AddWithValue("@KBSESK", string.IsNullOrEmpty(ItemMasterHistory.ResultRecordHandlingFlag) ? (object)DBNull.Value : ItemMasterHistory.ResultRecordHandlingFlag);                                   //成績書扱い区分
                    command.Parameters.AddWithValue("@FGMILL", string.IsNullOrEmpty(ItemMasterHistory.MillSheetRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.MillSheetRequiredFlag);                                         //ミルシート要否
                    command.Parameters.AddWithValue("@FGKYOD", string.IsNullOrEmpty(ItemMasterHistory.StrengthCalculationRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.StrengthCalculationRequiredFlag);                     //強度計算書有無
                    command.Parameters.AddWithValue("@FGKENT", string.IsNullOrEmpty(ItemMasterHistory.InspectionRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.InspectionRequiredFlag);                                       //検定要否
                    command.Parameters.AddWithValue("@FGSIJI", string.IsNullOrEmpty(ItemMasterHistory.InstructionRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.InstructionRequiredFlag);                                     //指示書要否
                    command.Parameters.AddWithValue("@FGZUMN", string.IsNullOrEmpty(ItemMasterHistory.DrawingRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.DrawingRequiredFlag);                                             //図面要否
                    command.Parameters.AddWithValue("@FGSHIY", string.IsNullOrEmpty(ItemMasterHistory.SpecificationRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.SpecificationRequiredFlag);                                 //仕様書要否
                    command.Parameters.AddWithValue("@FGNETU", string.IsNullOrEmpty(ItemMasterHistory.HeatTreatmentRecordRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.HeatTreatmentRecordRequiredFlag);                     //熱処理記録要否
                    command.Parameters.AddWithValue("@FGSUNK", string.IsNullOrEmpty(ItemMasterHistory.DimensionConfirmationRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.DimensionConfirmationRequiredFlag);                 //寸法確認書要否
                    command.Parameters.AddWithValue("@FGSETU", string.IsNullOrEmpty(ItemMasterHistory.InstructionManualRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.InstructionManualRequiredFlag);                         //取扱説明書要否
                    command.Parameters.AddWithValue("@FGKHKG", string.IsNullOrEmpty(ItemMasterHistory.KHKCertificateRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.KHKCertificateRequiredFlag);                               //KHK合格書要否
                    command.Parameters.AddWithValue("@FGKGSY", string.IsNullOrEmpty(ItemMasterHistory.HighPressureGasCertificationRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.HighPressureGasCertificationRequiredFlag);   //高圧ガス認定書要否
                    command.Parameters.AddWithValue("@FGPKAN", string.IsNullOrEmpty(ItemMasterHistory.ProcessManagementCode) ? (object)DBNull.Value : ItemMasterHistory.ProcessManagementCode);                                         //プロセス管理区分
                    command.Parameters.AddWithValue("@FGZKEN", string.IsNullOrEmpty(ItemMasterHistory.MaterialInspectionRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.MaterialInspectionRequiredFlag);                       //材料検査有無
                    command.Parameters.AddWithValue("@FGTANS", string.IsNullOrEmpty(ItemMasterHistory.SingleShippingFlag) ? (object)DBNull.Value : ItemMasterHistory.SingleShippingFlag);                                               //単体出荷可否
                    command.Parameters.AddWithValue("@FGZTOS", string.IsNullOrEmpty(ItemMasterHistory.MaterialCoatingRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.MaterialCoatingRequiredFlag);                             //材料時塗装要否
                    command.Parameters.AddWithValue("@FGZASK", string.IsNullOrEmpty(ItemMasterHistory.MaterialProvisionRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.MaterialProvisionRequiredFlag);                         //材料支給要否
                    command.Parameters.AddWithValue("@FGTJIG", string.IsNullOrEmpty(ItemMasterHistory.PressureTestingRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.PressureTestingRequiredFlag);                             //耐圧治具有無
                    command.Parameters.AddWithValue("@KBCYLI", string.IsNullOrEmpty(ItemMasterHistory.OldNewCylinderClassification) ? (object)DBNull.Value : ItemMasterHistory.OldNewCylinderClassification);                           //新旧シリンダ区分
                    command.Parameters.AddWithValue("@KBGKAK", string.IsNullOrEmpty(ItemMasterHistory.ProcessingClassification) ? (object)DBNull.Value : ItemMasterHistory.ProcessingClassification);                                   //加工区分
                    command.Parameters.AddWithValue("@KBTEHI", string.IsNullOrEmpty(ItemMasterHistory.ArrangementClassification) ? (object)DBNull.Value : ItemMasterHistory.ArrangementClassification);                                 //手配区分
                    command.Parameters.AddWithValue("@KBNYTS", string.IsNullOrEmpty(ItemMasterHistory.InboundManagementApplicableFlag) ? (object)DBNull.Value : ItemMasterHistory.InboundManagementApplicableFlag);                     //入荷管理対象区分
                    command.Parameters.AddWithValue("@KBNYKN", string.IsNullOrEmpty(ItemMasterHistory.InboundInspectionRequiredFlag) ? (object)DBNull.Value : ItemMasterHistory.InboundInspectionRequiredFlag);                         //入荷検査要否区分
                    command.Parameters.AddWithValue("@KBKNSA", string.IsNullOrEmpty(ItemMasterHistory.InspectionClassification) ? (object)DBNull.Value : ItemMasterHistory.InspectionClassification);                                   //検査区分
                    command.Parameters.AddWithValue("@QTLOTS", Convert.ToDecimal(ItemMasterHistory.LotQuantity));                                                                                                                       //ロット数量
                    command.Parameters.AddWithValue("@QTHCSU", Convert.ToDecimal(ItemMasterHistory.MinimumOrderQuantity));                                                                                                              //最小発注数量
                    command.Parameters.AddWithValue("@QTIMOJ", Convert.ToDecimal(ItemMasterHistory.CastingWeight));                                                                                                                     //鋳物単重
                    command.Parameters.AddWithValue("@PRIMOT", Convert.ToDecimal(ItemMasterHistory.CastingCost));                                                                                                                       //鋳物原価
                    command.Parameters.AddWithValue("@VLBSUN", string.IsNullOrEmpty(ItemMasterHistory.ComponentSizeLWH) ? (object)DBNull.Value : ItemMasterHistory.ComponentSizeLWH);                                                   //部品寸法LWH(mm)
                    command.Parameters.AddWithValue("@TXBSUN", string.IsNullOrEmpty(ItemMasterHistory.ComponentSizeRemarks) ? (object)DBNull.Value : ItemMasterHistory.ComponentSizeRemarks);                                           //部品寸法特記事項
                    command.Parameters.AddWithValue("@VLSUN1", Convert.ToDecimal(ItemMasterHistory.Dimension1));                                                                                                                        //寸法1
                    command.Parameters.AddWithValue("@VLSUN2", Convert.ToDecimal(ItemMasterHistory.Dimension2));                                                                                                                        //寸法2
                    command.Parameters.AddWithValue("@VLSUN3", Convert.ToDecimal(ItemMasterHistory.Dimension3));                                                                                                                        //寸法3
                    command.Parameters.AddWithValue("@QTLDTM", Convert.ToDecimal(ItemMasterHistory.ItemLT));                                                                                                                            //品目LT
                    command.Parameters.AddWithValue("@TXLTTM", string.IsNullOrEmpty(ItemMasterHistory.ItemLTRemarks) ? (object)DBNull.Value : ItemMasterHistory.ItemLTRemarks);                                                         //品目LT特記事項
                    command.Parameters.AddWithValue("@QTBUHJ", Convert.ToDecimal(ItemMasterHistory.ComponentSingleWeight));                                                                                                             //部品単体重量(kg)
                    command.Parameters.AddWithValue("@TXBUHJ", string.IsNullOrEmpty(ItemMasterHistory.ComponentSingleWeightRemarks) ? (object)DBNull.Value : ItemMasterHistory.ComponentSingleWeightRemarks);                           //部品単体重量特記事項
                    command.Parameters.AddWithValue("@QTTNJU", Convert.ToDecimal(ItemMasterHistory.UnitWeight));                                                                                                                        //単重
                    command.Parameters.AddWithValue("@CDDFSH", string.IsNullOrEmpty(ItemMasterHistory.StandardSupplierCode) ? (object)DBNull.Value : ItemMasterHistory.StandardSupplierCode);                                           //標準仕入先コード
                    command.Parameters.AddWithValue("@CDMEKA", string.IsNullOrEmpty(ItemMasterHistory.ManufacturerCode) ? (object)DBNull.Value : ItemMasterHistory.ManufacturerCode);                                                   //メーカーコード
                    command.Parameters.AddWithValue("@CDDAIR", string.IsNullOrEmpty(ItemMasterHistory.StandardAgentCode) ? (object)DBNull.Value : ItemMasterHistory.StandardAgentCode);                                                 //標準代理店コード
                    command.Parameters.AddWithValue("@TXMKAT", string.IsNullOrEmpty(ItemMasterHistory.ManufacturerModelNumber) ? (object)DBNull.Value : ItemMasterHistory.ManufacturerModelNumber);                                     //メーカー型番
                    command.Parameters.AddWithValue("@TXMEKA", string.IsNullOrEmpty(ItemMasterHistory.ManufacturerRemarks) ? (object)DBNull.Value : ItemMasterHistory.ManufacturerRemarks);                                             //メーカー特記事項
                    command.Parameters.AddWithValue("@TXBUHN", string.IsNullOrEmpty(ItemMasterHistory.ComponentDetail) ? (object)DBNull.Value : ItemMasterHistory.ComponentDetail);                                                     //部品詳細内容
                    command.Parameters.AddWithValue("@TXB512", string.IsNullOrEmpty(ItemMasterHistory.Remarks) ? (object)DBNull.Value : ItemMasterHistory.Remarks);                                                                     //特記事項
                    command.Parameters.AddWithValue("@CDSOKO", string.IsNullOrEmpty(ItemMasterHistory.WarehouseCode) ? (object)DBNull.Value : ItemMasterHistory.WarehouseCode);                                                         //倉庫コード
                    command.Parameters.AddWithValue("@KBHNKZ", string.IsNullOrEmpty(ItemMasterHistory.ItemTaxClassification) ? (object)DBNull.Value : ItemMasterHistory.ItemTaxClassification);                                         //品目課税区分
                    command.Parameters.AddWithValue("@KBKEIG", string.IsNullOrEmpty(ItemMasterHistory.ReducedTaxRateClassification) ? (object)DBNull.Value : ItemMasterHistory.ReducedTaxRateClassification);                           //軽減税率区分
                    command.Parameters.AddWithValue("@QTHCTN", Convert.ToDecimal(ItemMasterHistory.OrderPointQuantity));                                                                                                                //発注点数
                    command.Parameters.AddWithValue("@QTTZAI", Convert.ToDecimal(ItemMasterHistory.AppropriateStockQuantity));                                                                                                          //適正在庫数
                    command.Parameters.AddWithValue("@YMSIKO", Convert.ToDecimal(ItemMasterHistory.StartDateForPreparation));                                                                                                           //仕込開始年月
                    command.Parameters.AddWithValue("@QTHNEN", Convert.ToDecimal(ItemMasterHistory.AverageCalculationYears));                                                                                                           //平均算出年数
                    command.Parameters.AddWithValue("@RTKEPN", Convert.ToDecimal(ItemMasterHistory.StockOutToleranceRate));                                                                                                             //欠品許容率
                    command.Parameters.AddWithValue("@CDSRIS", string.IsNullOrEmpty(ItemMasterHistory.SeriesCode) ? (object)DBNull.Value : ItemMasterHistory.SeriesCode);                                                               //シリーズコード
                    command.Parameters.AddWithValue("@VLSIZE", Convert.ToDecimal(ItemMasterHistory.Size));                                                                                                                              //サイズ
                    command.Parameters.AddWithValue("@CDDCKA", string.IsNullOrEmpty(ItemMasterHistory.ReceivableCreditAccountCode) ? (object)DBNull.Value : ItemMasterHistory.ReceivableCreditAccountCode);                             //債権貸方科目コード
                    command.Parameters.AddWithValue("@CDDDKA", string.IsNullOrEmpty(ItemMasterHistory.PayableDebitAccountCode) ? (object)DBNull.Value : ItemMasterHistory.PayableDebitAccountCode);                                     //債権借方科目コード
                    command.Parameters.AddWithValue("@CDICKA", string.IsNullOrEmpty(ItemMasterHistory.LiabilityGeneralCreditAccountCode) ? (object)DBNull.Value : ItemMasterHistory.LiabilityGeneralCreditAccountCode);                 //債務(一般)貸方科目コード
                    command.Parameters.AddWithValue("@CDIDKA", string.IsNullOrEmpty(ItemMasterHistory.LiabilityGeneralDebitAccountCode) ? (object)DBNull.Value : ItemMasterHistory.LiabilityGeneralDebitAccountCode);                   //債務(一般)借方科目コード
                    command.Parameters.AddWithValue("@CDSCKA", string.IsNullOrEmpty(ItemMasterHistory.LiabilityManufacturingCreditAccountCode) ? (object)DBNull.Value : ItemMasterHistory.LiabilityManufacturingCreditAccountCode);     //債務(製造)貸方科目コード
                    command.Parameters.AddWithValue("@CDSDKA", string.IsNullOrEmpty(ItemMasterHistory.LiabilityManufacturingDebitAccountCode) ? (object)DBNull.Value : ItemMasterHistory.LiabilityManufacturingDebitAccountCode);       //債務(製造)借方科目コード
                    command.Parameters.AddWithValue("@PRSANK", Convert.ToDecimal(ItemMasterHistory.ReferencePrice));                                                                                                                    //参考単価
                    command.Parameters.AddWithValue("@CDKASH", string.IsNullOrEmpty(ItemMasterHistory.ProcessingDepartmentCode) ? (object)DBNull.Value : ItemMasterHistory.ProcessingDepartmentCode);                                   //加工担当部門
                    command.Parameters.AddWithValue("@ATKKOS", Convert.ToDecimal(ItemMasterHistory.ProcessingManHours));                                                                                                                //加工工数
                    command.Transaction = this.transaction;
                    command.ExecuteNonQuery();
                }
            }
        }

        public class ItemMasterStructureRepository
        {
            // *********************************************************************************
            // * 品目構成マスタのエンティティクラス
            // *********************************************************************************
            private OdbcConnection _connection;
            private OdbcTransaction _transaction;

            // コンストラクタ
            public ItemMasterStructureRepository(OdbcConnection connection, OdbcTransaction transaction)
            {
                _connection = connection;
                this._transaction = transaction;
            }

            // ItemMasterStructureクラスを定義
            public class ItemMasterStructure
            {
                public decimal DeleteFlag { get; set; } // 削除フラグ
                public string RegisteredUserCode { get; set; } // 登録担当者コード
                public decimal RegisteredDateTime { get; set; } // 登録日時
                public string RegisteredProgramId { get; set; } // 登録プログラムID
                public string UpdatedUserCode { get; set; } // 更新担当者コード
                public decimal UpdatedDateTime { get; set; } // 更新日時
                public string UpdatedProgramId { get; set; } // 更新プログラムID
                public string ParentItemCode { get; set; } // 親品目コード (PK)
                public decimal ItemStructureLineNo { get; set; } // 品目構成行No (PK)
                public decimal DisplayLineNo { get; set; } // 表示行No
                public string ChildItemCode { get; set; } // 子品目コード
                public decimal BalloonNumber { get; set; } // 風船番号
                public decimal Quantity { get; set; } // 員数
            }

            // 品目構成マスタを、親品目コード、品目構成行Noで検索し取得するメソッド
            public ItemMasterStructure GetItemStructureById(string parentItemCode, decimal itemStructureLineNo)
            {
                string query = "SELECT * FROM MHINK WHERE CDOHIN = ? AND NPHKSE = ?";

                using (OdbcCommand command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("CDOHIN", parentItemCode);
                    command.Parameters.AddWithValue("NPHKSE", itemStructureLineNo);
                    command.Transaction = this._transaction;

                    using (OdbcDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new ItemMasterStructure
                            {
                                DeleteFlag = reader.IsDBNull(0) ? 0 : reader.GetDecimal(0),
                                RegisteredUserCode = reader.IsDBNull(1) ? null : reader.GetString(1),
                                RegisteredDateTime = reader.IsDBNull(2) ? 0 : reader.GetDecimal(2),
                                RegisteredProgramId = reader.IsDBNull(3) ? null : reader.GetString(3),
                                UpdatedUserCode = reader.IsDBNull(4) ? null : reader.GetString(4),
                                UpdatedDateTime = reader.IsDBNull(5) ? 0 : reader.GetDecimal(5),
                                UpdatedProgramId = reader.IsDBNull(6) ? null : reader.GetString(6),
                                ParentItemCode = reader.GetString(7),
                                ItemStructureLineNo = reader.GetDecimal(8),
                                DisplayLineNo = reader.IsDBNull(9) ? 0 : reader.GetDecimal(9),
                                ChildItemCode = reader.IsDBNull(10) ? null : reader.GetString(10),
                                BalloonNumber = reader.IsDBNull(11) ? 0 : reader.GetDecimal(11),
                                Quantity = reader.IsDBNull(12) ? 0 : reader.GetDecimal(12)
                            };
                        }
                    }
                }
                return null; // 見つからなかった場合
            }

            // 品目構成マスタを挿入するメソッド
            public void InsertItemStructure(ItemMasterStructure itemStructure)
            {
                string query = "INSERT INTO MHINK (FGDELE, SPIUSR, SPIDTM, SPIPGM, SPUUSR, SPUDTM, SPUPGM, CDOHIN, NPHKSE, NPDSPL, CDKHIN, NOFSEN, QTINSU) " +
                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                using (OdbcCommand command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("FGDELE", itemStructure.DeleteFlag);
                    command.Parameters.AddWithValue("SPIUSR", string.IsNullOrEmpty(itemStructure.RegisteredUserCode) ? (object)DBNull.Value : itemStructure.RegisteredUserCode);
                    command.Parameters.AddWithValue("SPIDTM", itemStructure.RegisteredDateTime);
                    command.Parameters.AddWithValue("SPIPGM", string.IsNullOrEmpty(itemStructure.RegisteredProgramId) ? (object)DBNull.Value : itemStructure.RegisteredProgramId);
                    command.Parameters.AddWithValue("SPUUSR", string.IsNullOrEmpty(itemStructure.UpdatedUserCode) ? (object)DBNull.Value : itemStructure.UpdatedUserCode);
                    command.Parameters.AddWithValue("SPUDTM", itemStructure.UpdatedDateTime);
                    command.Parameters.AddWithValue("SPUPGM", string.IsNullOrEmpty(itemStructure.UpdatedProgramId) ? (object)DBNull.Value : itemStructure.UpdatedProgramId);
                    command.Parameters.AddWithValue("CDOHIN", itemStructure.ParentItemCode);
                    command.Parameters.AddWithValue("NPHKSE", itemStructure.ItemStructureLineNo);
                    command.Parameters.AddWithValue("NPDSPL", itemStructure.DisplayLineNo);
                    command.Parameters.AddWithValue("CDKHIN", string.IsNullOrEmpty(itemStructure.ChildItemCode) ? (object)DBNull.Value : itemStructure.ChildItemCode);
                    command.Parameters.AddWithValue("NOFSEN", itemStructure.BalloonNumber);
                    command.Parameters.AddWithValue("QTINSU", itemStructure.Quantity);
                    command.Transaction = this._transaction;
                    command.ExecuteNonQuery();
                }
            }

            // 品目構成マスタを削除するメソッド
            public void DeleteItemStructure(ItemMasterStructure itemStructure)
            {
                string query = "DELETE FROM MHINK WHERE CDOHIN = ?";

                using (OdbcCommand command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("CDOHIN", itemStructure.ParentItemCode);
                    command.Transaction = this._transaction;
                    command.ExecuteNonQuery();
                }
            }
        }

        public class DesignChangeFileRepository
        {
            // *********************************************************************************
            // * 設計変更ファイルのエンティティクラス
            // *********************************************************************************
            private OdbcConnection _connection;
            private OdbcTransaction _transaction;

            // コンストラクタ
            public DesignChangeFileRepository(OdbcConnection connection, OdbcTransaction transaction)
            {
                _connection = connection;
                _transaction = transaction;
            }

            // 設計変更ファイルを定義
            public class DesignChangeFile
            {
                public decimal DeleteFlag { get; set; } // 削除フラグ
                public string RegisteredUserCode { get; set; } // 登録担当者コード
                public decimal RegisteredDateTime { get; set; } // 登録日時
                public string RegisteredProgramId { get; set; } // 登録プログラムID
                public string UpdatedUserCode { get; set; } // 更新担当者コード
                public decimal UpdatedDateTime { get; set; } // 更新日時
                public string UpdatedProgramId { get; set; } // 更新プログラムID
                public string NotificationNumber { get; set; } // 設計変更通知番号
                public string NotificationCategory { get; set; } // 設計変更区分
                public string CodeNumber { get; set; } // コード番号
                public string BranchNumber { get; set; } // 枝番
                public string Name { get; set; } // 名称
                public string ModelCode { get; set; } // 型式コード
                public string NewItemCode { get; set; } // 新品目コード
                public string OldItemCode { get; set; } // 旧品目コード
                public string NewDrawingNumber { get; set; } // 新図面番号
                public string OldDrawingNumber { get; set; } // 旧図面番号
                public string Reason { get; set; } // 理由
                public string RevisionDocumentNumber { get; set; } // 改訂連絡書No
                public string FeedbackSheetNumber { get; set; } // フィードバックシートNo
                public string Item { get; set; } // 項目
                public string BOMChangeFlag { get; set; } // BOM構成変更フラグ
                public string ProcessingChangeFlag { get; set; } // 加工変更フラグ
                public string SpecificationReviewFlag { get; set; } // 認定仕様範囲の再確認
                public string DocumentReplacementFlag { get; set; } // 設計図書差し替え
                public string StockHandling { get; set; } // 仕掛品・在庫品の処理
                public string ChangeReasonContent { get; set; } // 変更理由・内容
                public string Creator { get; set; } // 作成者
                public decimal CreationDate { get; set; } // 作成日時
                public string Checker { get; set; } // 検図者
                public decimal CheckDate { get; set; } // 検図日
                public string Approver { get; set; } // 承認者
                public decimal ApprovalDate { get; set; } // 承認日
            }

            // 設計変更ファイルを、設計変更通知書番号で検索し取得するメソッド
            public DesignChangeFile GetDesignChangeFileById(string notificationNumber)
            {
                string query = "SELECT * FROM FSEHN WHERE NOSKHN = ?";

                using (OdbcCommand command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("NOSKHN", notificationNumber);
                    command.Transaction = this._transaction;
                    using (OdbcDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new DesignChangeFile
                            {
                                DeleteFlag = reader.IsDBNull(0) ? 0 : reader.GetDecimal(0),
                                RegisteredUserCode = reader.IsDBNull(1) ? null : reader.GetString(1),
                                RegisteredDateTime = reader.IsDBNull(2) ? 0 : reader.GetDecimal(2),
                                RegisteredProgramId = reader.IsDBNull(3) ? null : reader.GetString(3),
                                UpdatedUserCode = reader.IsDBNull(4) ? null : reader.GetString(4),
                                UpdatedDateTime = reader.IsDBNull(5) ? 0 : reader.GetDecimal(5),
                                UpdatedProgramId = reader.IsDBNull(6) ? null : reader.GetString(6),
                                NotificationNumber = reader.GetString(7),
                                NotificationCategory = reader.IsDBNull(8) ? null : reader.GetString(8),
                                CodeNumber = reader.IsDBNull(9) ? null : reader.GetString(9),
                                BranchNumber = reader.IsDBNull(10) ? null : reader.GetString(10),
                                Name = reader.IsDBNull(11) ? null : reader.GetString(11),
                                ModelCode = reader.IsDBNull(12) ? null : reader.GetString(12),
                                NewItemCode = reader.IsDBNull(13) ? null : reader.GetString(13),
                                OldItemCode = reader.IsDBNull(14) ? null : reader.GetString(14),
                                NewDrawingNumber = reader.IsDBNull(15) ? null : reader.GetString(15),
                                OldDrawingNumber = reader.IsDBNull(16) ? null : reader.GetString(16),
                                Reason = reader.IsDBNull(17) ? null : reader.GetString(17),
                                RevisionDocumentNumber = reader.IsDBNull(18) ? null : reader.GetString(18),
                                FeedbackSheetNumber = reader.IsDBNull(19) ? null : reader.GetString(19),
                                Item = reader.IsDBNull(20) ? null : reader.GetString(20),
                                BOMChangeFlag = reader.IsDBNull(21) ? null : reader.GetString(21),
                                ProcessingChangeFlag = reader.IsDBNull(22) ? null : reader.GetString(22),
                                SpecificationReviewFlag = reader.IsDBNull(23) ? null : reader.GetString(23),
                                DocumentReplacementFlag = reader.IsDBNull(24) ? null : reader.GetString(24),
                                StockHandling = reader.IsDBNull(25) ? null : reader.GetString(25),
                                ChangeReasonContent = reader.IsDBNull(26) ? null : reader.GetString(26),
                                Creator = reader.IsDBNull(27) ? null : reader.GetString(27),
                                CreationDate = reader.IsDBNull(28) ? 0 : reader.GetDecimal(28),
                                Checker = reader.IsDBNull(29) ? null : reader.GetString(29),
                                CheckDate = reader.IsDBNull(30) ? 0 : reader.GetDecimal(30),
                                Approver = reader.IsDBNull(31) ? null : reader.GetString(31),
                                ApprovalDate = reader.IsDBNull(32) ? 0 : reader.GetDecimal(32),
                            };
                        }
                    }
                }
                return null; // 見つからなかった場合
            }

            // 設計変更ファイルを挿入するメソッド
            public void InsertDesignChangeFile(DesignChangeFile designChangeFile)
            {
                string query = @"INSERT 
                                    INTO FSEHN( 
                                          FGDELE                                    -- 削除フラグ
                                        , SPIUSR                                    -- 登録担当者コード
                                        , SPIDTM                                    -- 登録日時
                                        , SPIPGM                                    -- 登録プログラムID
                                        , SPUUSR                                    -- 更新担当者コード
                                        , SPUDTM                                    -- 更新日時
                                        , SPUPGM                                    -- 更新プログラムID
                                        , NOSKHN                                    -- 設計変更通知番号
                                        , KBSKHN                                    -- 設計変更区分
                                        , NOCODE                                    -- コード番号
                                        , NOCDE1                                    -- 枝番
                                        , NMSEHN                                    -- 名称
                                        , CDKISH                                    -- 型式コード
                                        , CDHIN2                                    -- 新品目コード
                                        , CDHIN1                                    -- 旧品目コード
                                        , VLZUB2                                    -- 新図面番号
                                        , VLZUB1                                    -- 旧図面番号
                                        , TXRIYU                                    -- 理由
                                        , NOKAIT                                    -- 改訂連絡書No
                                        , NOFEED                                    -- フィードバックシー
                                        , TXKOMK                                    -- 項目
                                        , FGKOHN                                    -- BOM構成変更フラグ
                                        , FGKAKO                                    -- 加工変更フラグ
                                        , FGSAIK                                    -- 認定仕様範囲の再確
                                        , FGTOSY                                    -- 設計図書差し替え
                                        , TXZSYO                                    -- 仕掛品(H/B/S/A/KK
                                        , TXHENK                                    -- 変更理由・内容
                                        , NMSAKU                                    -- 作成者
                                        , DTSAKU                                    -- 作成日時
                                        , NMKNZU                                    -- 検図者
                                        , DTKNZU                                    -- 検図日
                                        , NMSNIN                                    -- 承認者
                                        , DTSNIN                                    -- 承認日
                                    ) 
                                    VALUES ( 
                                          ?                                     -- 削除フラグ
                                        , ?                                   -- 登録担当者コード
                                        , ?                                   -- 登録日時
                                        , ?                                   -- 登録プログラムID
                                        , ?                                   -- 更新担当者コード
                                        , ?                                   -- 更新日時
                                        , ?                                   -- 更新プログラムID
                                        , ?                                   -- 設計変更通知番号
                                        , ?                                   -- 設計変更区分
                                        , ?                                   -- コード番号
                                        , ?                                   -- 枝番
                                        , ?                                   -- 名称
                                        , ?                                   -- 型式コード
                                        , ?                                   -- 新品目コード
                                        , ?                                   -- 旧品目コード
                                        , ?                                   -- 新図面番号
                                        , ?                                   -- 旧図面番号
                                        , ?                                   -- 理由
                                        , ?                                   -- 改訂連絡書No
                                        , ?                                   -- フィードバックシー
                                        , ?                                   -- 項目
                                        , ?                                   -- BOM構成変更フラグ
                                        , ?                                   -- 加工変更フラグ
                                        , ?                                   -- 認定仕様範囲の再確
                                        , ?                                   -- 設計図書差し替え
                                        , ?                                   -- 仕掛品(H/B/S/A/KK
                                        , ?                                   -- 変更理由・内容
                                        , ?                                   -- 作成者
                                        , ?                                   -- 作成日時
                                        , ?                                   -- 検図者
                                        , ?                                   -- 検図日
                                        , ?                                   -- 承認者
                                        , ?                                   -- 承認日
                                    )";

                using (OdbcCommand command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("FGDELE", designChangeFile.DeleteFlag);
                    command.Parameters.AddWithValue("SPIUSR", string.IsNullOrEmpty(designChangeFile.RegisteredUserCode) ? (object)DBNull.Value : designChangeFile.RegisteredUserCode);
                    command.Parameters.AddWithValue("SPIDTM", designChangeFile.RegisteredDateTime);
                    command.Parameters.AddWithValue("SPIPGM", string.IsNullOrEmpty(designChangeFile.RegisteredProgramId) ? (object)DBNull.Value : designChangeFile.RegisteredProgramId);
                    command.Parameters.AddWithValue("SPUUSR", string.IsNullOrEmpty(designChangeFile.UpdatedUserCode) ? (object)DBNull.Value : designChangeFile.UpdatedUserCode);
                    command.Parameters.AddWithValue("SPUDTM", designChangeFile.UpdatedDateTime);
                    command.Parameters.AddWithValue("SPUPGM", string.IsNullOrEmpty(designChangeFile.UpdatedProgramId) ? (object)DBNull.Value : designChangeFile.UpdatedProgramId);
                    command.Parameters.AddWithValue("NOSKHN", designChangeFile.NotificationNumber);
                    command.Parameters.AddWithValue("KBSKHN", string.IsNullOrEmpty(designChangeFile.NotificationCategory) ? (object)DBNull.Value : designChangeFile.NotificationCategory);
                    command.Parameters.AddWithValue("NOCODE", string.IsNullOrEmpty(designChangeFile.CodeNumber) ? (object)DBNull.Value : designChangeFile.CodeNumber);
                    command.Parameters.AddWithValue("NOCDE1", string.IsNullOrEmpty(designChangeFile.BranchNumber) ? (object)DBNull.Value : designChangeFile.BranchNumber);
                    command.Parameters.AddWithValue("NMSEHN", string.IsNullOrEmpty(designChangeFile.Name) ? (object)DBNull.Value : designChangeFile.Name);
                    command.Parameters.AddWithValue("CDKISH", string.IsNullOrEmpty(designChangeFile.ModelCode) ? (object)DBNull.Value : designChangeFile.ModelCode);
                    command.Parameters.AddWithValue("CDHIN2", string.IsNullOrEmpty(designChangeFile.NewItemCode) ? (object)DBNull.Value : designChangeFile.NewItemCode);
                    command.Parameters.AddWithValue("CDHIN1", string.IsNullOrEmpty(designChangeFile.OldItemCode) ? (object)DBNull.Value : designChangeFile.OldItemCode);
                    command.Parameters.AddWithValue("VLZUB2", string.IsNullOrEmpty(designChangeFile.NewDrawingNumber) ? (object)DBNull.Value : designChangeFile.NewDrawingNumber);
                    command.Parameters.AddWithValue("VLZUB1", string.IsNullOrEmpty(designChangeFile.OldDrawingNumber) ? (object)DBNull.Value : designChangeFile.OldDrawingNumber);
                    command.Parameters.AddWithValue("TXRIYU", string.IsNullOrEmpty(designChangeFile.Reason) ? (object)DBNull.Value : designChangeFile.Reason);
                    command.Parameters.AddWithValue("NOKAIT", string.IsNullOrEmpty(designChangeFile.RevisionDocumentNumber) ? (object)DBNull.Value : designChangeFile.RevisionDocumentNumber);
                    command.Parameters.AddWithValue("NOFEED", string.IsNullOrEmpty(designChangeFile.FeedbackSheetNumber) ? (object)DBNull.Value : designChangeFile.FeedbackSheetNumber);
                    command.Parameters.AddWithValue("TXKOMK", string.IsNullOrEmpty(designChangeFile.Item) ? (object)DBNull.Value : designChangeFile.Item);
                    command.Parameters.AddWithValue("FGKOHN", string.IsNullOrEmpty(designChangeFile.BOMChangeFlag) ? (object)DBNull.Value : designChangeFile.BOMChangeFlag);
                    command.Parameters.AddWithValue("FGKAKO", string.IsNullOrEmpty(designChangeFile.ProcessingChangeFlag) ? (object)DBNull.Value : designChangeFile.ProcessingChangeFlag);
                    command.Parameters.AddWithValue("FGSAIK", string.IsNullOrEmpty(designChangeFile.SpecificationReviewFlag) ? (object)DBNull.Value : designChangeFile.SpecificationReviewFlag);
                    command.Parameters.AddWithValue("FGTOSY", string.IsNullOrEmpty(designChangeFile.DocumentReplacementFlag) ? (object)DBNull.Value : designChangeFile.DocumentReplacementFlag);
                    command.Parameters.AddWithValue("TXZSYO", string.IsNullOrEmpty(designChangeFile.StockHandling) ? (object)DBNull.Value : designChangeFile.StockHandling);
                    command.Parameters.AddWithValue("TXHENK", string.IsNullOrEmpty(designChangeFile.ChangeReasonContent) ? (object)DBNull.Value : designChangeFile.ChangeReasonContent);
                    command.Parameters.AddWithValue("NMSAKU", string.IsNullOrEmpty(designChangeFile.Creator) ? (object)DBNull.Value : designChangeFile.Creator);
                    command.Parameters.AddWithValue("DTSAKU", designChangeFile.CreationDate);
                    command.Parameters.AddWithValue("NMKNZU", string.IsNullOrEmpty(designChangeFile.Checker) ? (object)DBNull.Value : designChangeFile.Checker);
                    command.Parameters.AddWithValue("DTKNZU", designChangeFile.CheckDate);
                    command.Parameters.AddWithValue("NMSNIN", string.IsNullOrEmpty(designChangeFile.Approver) ? (object)DBNull.Value : designChangeFile.Approver);
                    command.Parameters.AddWithValue("DTSNIN", designChangeFile.ApprovalDate);
                    command.Transaction = this._transaction;
                    command.ExecuteNonQuery();
                }
            }

            // 設計変更ファイルを変更するメソッド
            public void UpdateDesignChangeFile(DesignChangeFile designChangeFile)
            {
                string query = "UPDATE FSEHN SET " +
                               "FGDELE = ?, " +
                               "SPIUSR = ?, " +
                               "SPIDTM = ?, " +
                               "SPIPGM = ?, " +
                               "SPUUSR = ?, " +
                               "SPUDTM = ?, " +
                               "SPUPGM = ?, " +
                               "KBSKHN = ?, " +
                               "NOCODE = ?, " +
                               "NOCDE1 = ?, " +
                               "NMSEHN = ?, " +
                               "CDKISH = ?, " +
                               "CDHIN2 = ?, " +
                               "CDHIN1 = ?, " +
                               "VLZUB2 = ?, " +
                               "VLZUB1 = ?, " +
                               "TXRIYU = ?, " +
                               "NOKAIT = ?, " +
                               "NOFEED = ?, " +
                               "TXKOMK = ?, " +
                               "FGKOHN = ?, " +
                               "FGKAKO = ?, " +
                               "FGSAIK = ?, " +
                               "FGTOSY = ?, " +
                               "TXZSYO = ?, " +
                               "TXHENK = ?, " +
                               "NMSAKU = ?, " +
                               "DTSAKU = ?, " +
                               "NMKNZU = ?, " +
                               "DTKNZU = ?, " +
                               "NMSNIN = ?, " +
                               "DTSNIN = ? " +
                               "WHERE NOSKHN = ?";
                //
                using (OdbcCommand command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("FGDELE", designChangeFile.DeleteFlag);
                    command.Parameters.AddWithValue("SPIUSR", string.IsNullOrEmpty(designChangeFile.RegisteredUserCode) ? (object)DBNull.Value : designChangeFile.RegisteredUserCode);
                    command.Parameters.AddWithValue("SPIDTM", designChangeFile.RegisteredDateTime);
                    command.Parameters.AddWithValue("SPIPGM", string.IsNullOrEmpty(designChangeFile.RegisteredProgramId) ? (object)DBNull.Value : designChangeFile.RegisteredProgramId);
                    command.Parameters.AddWithValue("SPUUSR", string.IsNullOrEmpty(designChangeFile.UpdatedUserCode) ? (object)DBNull.Value : designChangeFile.UpdatedUserCode);
                    command.Parameters.AddWithValue("SPUDTM", designChangeFile.UpdatedDateTime);
                    command.Parameters.AddWithValue("SPUPGM", string.IsNullOrEmpty(designChangeFile.UpdatedProgramId) ? (object)DBNull.Value : designChangeFile.UpdatedProgramId);

                    command.Parameters.AddWithValue("KBSKHN", string.IsNullOrEmpty(designChangeFile.NotificationCategory) ? (object)DBNull.Value : designChangeFile.NotificationCategory);
                    command.Parameters.AddWithValue("NOCODE", string.IsNullOrEmpty(designChangeFile.CodeNumber) ? (object)DBNull.Value : designChangeFile.CodeNumber);
                    command.Parameters.AddWithValue("NOCDE1", string.IsNullOrEmpty(designChangeFile.BranchNumber) ? (object)DBNull.Value : designChangeFile.BranchNumber);
                    command.Parameters.AddWithValue("NMSEHN", string.IsNullOrEmpty(designChangeFile.Name) ? (object)DBNull.Value : designChangeFile.Name);
                    command.Parameters.AddWithValue("CDKISH", string.IsNullOrEmpty(designChangeFile.ModelCode) ? (object)DBNull.Value : designChangeFile.ModelCode);
                    command.Parameters.AddWithValue("CDHIN2", string.IsNullOrEmpty(designChangeFile.NewItemCode) ? (object)DBNull.Value : designChangeFile.NewItemCode);
                    command.Parameters.AddWithValue("CDHIN1", string.IsNullOrEmpty(designChangeFile.OldItemCode) ? (object)DBNull.Value : designChangeFile.OldItemCode);
                    command.Parameters.AddWithValue("VLZUB2", string.IsNullOrEmpty(designChangeFile.NewDrawingNumber) ? (object)DBNull.Value : designChangeFile.NewDrawingNumber);
                    command.Parameters.AddWithValue("VLZUB1", string.IsNullOrEmpty(designChangeFile.OldDrawingNumber) ? (object)DBNull.Value : designChangeFile.OldDrawingNumber);
                    command.Parameters.AddWithValue("TXRIYU", string.IsNullOrEmpty(designChangeFile.Reason) ? (object)DBNull.Value : designChangeFile.Reason);
                    command.Parameters.AddWithValue("NOKAIT", string.IsNullOrEmpty(designChangeFile.RevisionDocumentNumber) ? (object)DBNull.Value : designChangeFile.RevisionDocumentNumber);
                    command.Parameters.AddWithValue("NOFEED", string.IsNullOrEmpty(designChangeFile.FeedbackSheetNumber) ? (object)DBNull.Value : designChangeFile.FeedbackSheetNumber);
                    command.Parameters.AddWithValue("TXKOMK", string.IsNullOrEmpty(designChangeFile.Item) ? (object)DBNull.Value : designChangeFile.Item);
                    command.Parameters.AddWithValue("FGKOHN", string.IsNullOrEmpty(designChangeFile.BOMChangeFlag) ? (object)DBNull.Value : designChangeFile.BOMChangeFlag);
                    command.Parameters.AddWithValue("FGKAKO", string.IsNullOrEmpty(designChangeFile.ProcessingChangeFlag) ? (object)DBNull.Value : designChangeFile.ProcessingChangeFlag);
                    command.Parameters.AddWithValue("FGSAIK", string.IsNullOrEmpty(designChangeFile.SpecificationReviewFlag) ? (object)DBNull.Value : designChangeFile.SpecificationReviewFlag);
                    command.Parameters.AddWithValue("FGTOSY", string.IsNullOrEmpty(designChangeFile.DocumentReplacementFlag) ? (object)DBNull.Value : designChangeFile.DocumentReplacementFlag);
                    command.Parameters.AddWithValue("TXZSYO", string.IsNullOrEmpty(designChangeFile.StockHandling) ? (object)DBNull.Value : designChangeFile.StockHandling);
                    command.Parameters.AddWithValue("TXHENK", string.IsNullOrEmpty(designChangeFile.ChangeReasonContent) ? (object)DBNull.Value : designChangeFile.ChangeReasonContent);
                    command.Parameters.AddWithValue("NMSAKU", string.IsNullOrEmpty(designChangeFile.Creator) ? (object)DBNull.Value : designChangeFile.Creator);
                    command.Parameters.AddWithValue("DTSAKU", designChangeFile.CreationDate);
                    command.Parameters.AddWithValue("NMKNZU", string.IsNullOrEmpty(designChangeFile.Checker) ? (object)DBNull.Value : designChangeFile.Checker);
                    command.Parameters.AddWithValue("DTKNZU", designChangeFile.CheckDate);
                    command.Parameters.AddWithValue("NMSNIN", string.IsNullOrEmpty(designChangeFile.Approver) ? (object)DBNull.Value : designChangeFile.Approver);
                    command.Parameters.AddWithValue("DTSNIN", designChangeFile.ApprovalDate);
                    command.Parameters.AddWithValue("NOSKHN", designChangeFile.NotificationNumber); // WHERE句用
                    command.Transaction = this._transaction;
                    command.ExecuteNonQuery();
                }
            }
        }

        public interface IBaseRightHistoryDetail
        {
            // *********************************************************************************
            // * BaseRight連携明細のインターフェースクラス
            // *********************************************************************************
            void Insert(OdbcConnection connection, string baseRightNo);
        }

        public class BaseRightHistoryHeader
        {
            // *********************************************************************************
            // * BaseRight連携見出ファイルクラス
            // *********************************************************************************
            // Properties for BaseRight連携履歴見出ファイル
            public int DeleteFlag { get; set; }
            public string RegisteredUserCode { get; set; }
            public long RegisteredDateTime { get; set; }
            public string RegisteredProgramID { get; set; }
            public string UpdatedUserCode { get; set; }
            public long UpdatedDateTime { get; set; }
            public string UpdatedProgramID { get; set; }
            public string BaseRightNo { get; set; }       // NOBRRN
            public string BaseRightCategory { get; set; } // KBBRRN
            public long LinkedDate { get; set; }      // DTBRRN
            public int Sequence { get; set; }              // NOSEQ
            public string Status { get; set; }             // STRENK
            public string Comment { get; set; }            // TXCOMT
            public string SourceFilePath { get; set; }     // VLPTBF
            public string DestinationFilePath { get; set; } // VLPTAF

            // List of detail items (1:n relationship)
            public List<IBaseRightHistoryDetail> Details { get; set; } = new List<IBaseRightHistoryDetail>();
            // Method to insert header into the database
            public void Insert(OdbcConnection connection)
            {
                string headerQuery = @"
                                    INSERT INTO FHBRR (
                                        FGDELE, 
                                        SPIUSR, 
                                        SPIDTM, 
                                        SPIPGM, 
                                        SPUUSR, 
                                        SPUDTM, 
                                        SPUPGM,
                                        NOBRRN, 
                                        KBBRRN, 
                                        DTBRRN, 
                                        NOSEQ, 
                                        STRENK, 
                                        TXCOMT, 
                                        VLPTBF, 
                                        VLPTAF
                                    ) VALUES (
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ? ,
                                        ?
                                        )";
                //
                using (OdbcCommand command = new OdbcCommand(headerQuery, connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", this.DeleteFlag);
                    command.Parameters.AddWithValue("@SPIUSR", this.RegisteredUserCode);
                    command.Parameters.AddWithValue("@SPIDTM", this.RegisteredDateTime);
                    command.Parameters.AddWithValue("@SPIPGM", this.RegisteredProgramID);
                    command.Parameters.AddWithValue("@SPUUSR", this.UpdatedUserCode);
                    command.Parameters.AddWithValue("@SPUDTM", this.UpdatedDateTime);
                    command.Parameters.AddWithValue("@SPUPGM", this.UpdatedProgramID);
                    command.Parameters.AddWithValue("@NOBRRN", this.BaseRightNo);
                    command.Parameters.AddWithValue("@KBBRRN", this.BaseRightCategory);
                    command.Parameters.AddWithValue("@DTBRRN", this.LinkedDate);
                    command.Parameters.AddWithValue("@NOSEQ", this.Sequence);
                    command.Parameters.AddWithValue("@STRENK", this.Status);
                    command.Parameters.AddWithValue("@TXCOMT", this.Comment);
                    command.Parameters.AddWithValue("@VLPTBF", this.SourceFilePath);
                    command.Parameters.AddWithValue("@VLPTAF", this.DestinationFilePath);

                    // Execute the Insert command for header
                    command.ExecuteNonQuery();

                    // Insert details
                    foreach (var detail in Details)
                    {
                        detail.Insert(connection, this.BaseRightNo); // Pass the parent key to the detail insert
                    }
                }
            }
        }

        public class BaseRightHistoryDetailItem : IBaseRightHistoryDetail
        {
            // Properties for BaseRight連携履歴明細ファイル(品目)
            public int DeleteFlag { get; set; }
            public string RegisteredUserCode { get; set; }
            public long RegisteredDateTime { get; set; }
            public string RegisteredProgramID { get; set; }
            public string UpdatedUserCode { get; set; }
            public long UpdatedDateTime { get; set; }
            public string UpdatedProgramID { get; set; }
            public string BaseRightNo { get; set; }         // NOBRRN
            public int ItemLineNo { get; set; }              // NPBRRH
            public string DesignNo { get; set; }             // TXBR01
            public string ChangeSymbol { get; set; }         // TXBR02
            public string ItemNo { get; set; }               // TXBR03
            public string StandardName_JP { get; set; }      // TXBR04
            public string StandardName_EN { get; set; }      // TXBR05
            public string UsageForbiddenCategory { get; set; } // TXBR06
            public string ChangedItemNo { get; set; }        // TXBR07
            public string ClassName { get; set; }            // TXBR08
            public string ItemLinkCategory { get; set; }     // TXBR09
            public string AssemblyCategory { get; set; }      // TXBR10
            public string SpeciesCode { get; set; }          // TXBR11
            public string MaterialCode { get; set; }         // TXBR12
            public string Remarks { get; set; }               // TXBR13

            // Method to insert detail into the database
            public void Insert(OdbcConnection connection, string baseRightNo)
            {
                string detailQuery = @"
                                    INSERT INTO FBBRH (
                                        FGDELE, 
                                        SPIUSR, 
                                        SPIDTM, 
                                        SPIPGM, 
                                        SPUUSR, 
                                        SPUDTM, 
                                        SPUPGM,
                                        NOBRRN, 
                                        NPBRRH, 
                                        TXBR01, 
                                        TXBR02, 
                                        TXBR03, 
                                        TXBR04, 
                                        TXBR05, 
                                        TXBR06, 
                                        TXBR07, 
                                        TXBR08, 
                                        TXBR09, 
                                        TXBR10, 
                                        TXBR11, 
                                        TXBR12, 
                                        TXBR13
                                    ) VALUES (
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?, 
                                        ?  
                                    )";

                using (OdbcCommand command = new OdbcCommand(detailQuery, connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", this.DeleteFlag);
                    command.Parameters.AddWithValue("@SPIUSR", this.RegisteredUserCode);
                    command.Parameters.AddWithValue("@SPIDTM", this.RegisteredDateTime);
                    command.Parameters.AddWithValue("@SPIPGM", this.RegisteredProgramID);
                    command.Parameters.AddWithValue("@SPUUSR", this.UpdatedUserCode);
                    command.Parameters.AddWithValue("@SPUDTM", this.UpdatedDateTime);
                    command.Parameters.AddWithValue("@SPUPGM", this.UpdatedProgramID);
                    command.Parameters.AddWithValue("@NOBRRN", this.BaseRightNo);
                    command.Parameters.AddWithValue("@NPBRRH", this.ItemLineNo);
                    command.Parameters.AddWithValue("@TXBR01", this.DesignNo);
                    command.Parameters.AddWithValue("@TXBR02", this.ChangeSymbol);
                    command.Parameters.AddWithValue("@TXBR03", this.ItemNo);
                    command.Parameters.AddWithValue("@TXBR04", this.StandardName_JP);
                    command.Parameters.AddWithValue("@TXBR05", this.StandardName_EN);
                    command.Parameters.AddWithValue("@TXBR06", this.UsageForbiddenCategory);
                    command.Parameters.AddWithValue("@TXBR07", this.ChangedItemNo);
                    command.Parameters.AddWithValue("@TXBR08", this.ClassName);
                    command.Parameters.AddWithValue("@TXBR09", this.ItemLinkCategory);
                    command.Parameters.AddWithValue("@TXBR10", this.AssemblyCategory);
                    command.Parameters.AddWithValue("@TXBR11", this.SpeciesCode);
                    command.Parameters.AddWithValue("@TXBR12", this.MaterialCode);
                    command.Parameters.AddWithValue("@TXBR13", this.Remarks);

                    // Execute the Insert command for detail
                    command.ExecuteNonQuery();
                }
            }
        }

        public class BaseRightHistoryDetailItemStructure : IBaseRightHistoryDetail
        {
            // Properties for BaseRight連携履歴明細ファイル(品目構成)
            public int DeleteFlag { get; set; }
            public string RegisteredUserCode { get; set; }
            public long RegisteredDateTime { get; set; }
            public string RegisteredProgramID { get; set; }
            public string UpdatedUserCode { get; set; }
            public long UpdatedDateTime { get; set; }
            public string UpdatedProgramID { get; set; }
            //
            public string BaseRightNo { get; set; }             // NOBRRN
            public int ItemLineNo { get; set; }                 // NPBRRH
            public string parentItemNumber { get; set; }        // TXBR01
            public string balloonNumber { get; set; }           // TXBR02
            public string order { get; set; }                   // TXBR03
            public string childItemNumber { get; set; }         // TXBR04
            public string quantity { get; set; }                // TXBR05

            // Method to insert detail into the database
            public void Insert(OdbcConnection connection, string baseRightNo)
            {
                string detailQuery = @"
                                    INSERT 
                                    INTO FBBRK( 
                                          FGDELE           -- 削除フラグ
                                        , SPIUSR           -- 登録担当者コード
                                        , SPIDTM           -- 登録日時
                                        , SPIPGM           -- 登録プログラムID
                                        , SPUUSR           -- 更新担当者コード
                                        , SPUDTM           -- 更新日時
                                        , SPUPGM           -- 更新プログラムID
                                        , NOBRRN           -- BR連携No
                                        , NPBRRH           -- BR連携品目行No
                                        , TXBR01           -- 親品目番号
                                        , TXBR02           -- 風船番号(照番)
                                        , TXBR03           -- 順序
                                        , TXBR04           -- 子品目番号
                                        , TXBR05           -- 数量
                                    ) 
                                    VALUES ( 
                                          ?                -- 削除フラグ
                                        , ?                -- 登録担当者コード
                                        , ?                -- 登録日時
                                        , ?                -- 登録プログラムID
                                        , ?                -- 更新担当者コード
                                        , ?                -- 更新日時
                                        , ?                -- 更新プログラムID
                                        , ?                -- BR連携No
                                        , ?                -- BR連携品目行No
                                        , ?                -- 親品目番号
                                        , ?                -- 風船番号(照番)
                                        , ?                -- 順序
                                        , ?                -- 子品目番号
                                        , ?                -- 数量
                                    )";

                using (OdbcCommand command = new OdbcCommand(detailQuery, connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", this.DeleteFlag);
                    command.Parameters.AddWithValue("@SPIUSR", this.RegisteredUserCode);
                    command.Parameters.AddWithValue("@SPIDTM", this.RegisteredDateTime);
                    command.Parameters.AddWithValue("@SPIPGM", this.RegisteredProgramID);
                    command.Parameters.AddWithValue("@SPUUSR", this.UpdatedUserCode);
                    command.Parameters.AddWithValue("@SPUDTM", this.UpdatedDateTime);
                    command.Parameters.AddWithValue("@SPUPGM", this.UpdatedProgramID);
                    command.Parameters.AddWithValue("@NOBRRN", this.BaseRightNo);
                    command.Parameters.AddWithValue("@NPBRRH", this.ItemLineNo);
                    command.Parameters.AddWithValue("@TXBR01", this.parentItemNumber);
                    command.Parameters.AddWithValue("@TXBR02", this.balloonNumber);
                    command.Parameters.AddWithValue("@TXBR03", this.order);
                    command.Parameters.AddWithValue("@TXBR04", this.childItemNumber);
                    command.Parameters.AddWithValue("@TXBR05", this.quantity);

                    // Execute the Insert command for detail
                    command.ExecuteNonQuery();
                }
            }
        }

        public class BaseRightHistoryDetailDesignChange : IBaseRightHistoryDetail
        {
            // Properties for BaseRight連携履歴明細ファイル(設計変更)
            public int DeleteFlag { get; set; }
            public string RegisteredUserCode { get; set; }
            public long RegisteredDateTime { get; set; }
            public string RegisteredProgramID { get; set; }
            public string UpdatedUserCode { get; set; }
            public long UpdatedDateTime { get; set; }
            public string UpdatedProgramID { get; set; }
            //
            public string BaseRightNo { get; set; }                     //NOBRRN
            public int ItemLineNo { get; set; }                         //NPBRRH
            public string NotificationNumber { get; set; }				//TXBR01
            public string Classification { get; set; }                  //TXBR02
            public string CodeNumber { get; set; }                      //TXBR03
            public string Name { get; set; }                            //TXBR04
            public string Model { get; set; }                           //TXBR05
            public string NewItemNumber { get; set; }                   //TXBR06
            public string OldItemNumber { get; set; }                   //TXBR07
            public string NewDrawingNumber { get; set; }                //TXBR08
            public string OldDrawingNumber { get; set; }                //TXBR09
            public string Reason { get; set; }                          //TXBR10
            public string RevisionContactNo { get; set; }               //TXBR11
            public string FeedbackSheetNo { get; set; }                 //TXBR12
            public string Item { get; set; }                            //TXBR13
            public string BOMChange { get; set; }                       //TXBR14
            public string ProcessingChange { get; set; }                //TXBR15
            public string SpecificationReview { get; set; }             //TXBR16
            public string DocumentReplacement { get; set; }             //TXBR17
            public string StockHandling { get; set; }                   //TXBR18
            public string ChangeReasonAndContent { get; set; }          //TXBR19
            public string Creator { get; set; }                         //TXBR20
            public string CreationDate { get; set; }                      //TXBR21
            public string Checker { get; set; }                         //TXBR22
            public string CheckDate { get; set; }                         //TXBR23
            public string Approver { get; set; }                        //TXBR24
            public string ApprovalDate { get; set; }                      //TXBR25


            // Method to insert detail into the database
            public void Insert(OdbcConnection connection, string baseRightNo)
            {
                string detailQuery = @"
                                    INSERT 
                                    INTO FBBRS( 
                                          FGDELE                                    -- 削除フラグ
                                        , SPIUSR                                    -- 登録担当者コード
                                        , SPIDTM                                    -- 登録日時
                                        , SPIPGM                                    -- 登録プログラムID
                                        , SPUUSR                                    -- 更新担当者コード
                                        , SPUDTM                                    -- 更新日時
                                        , SPUPGM                                    -- 更新プログラムID
                                        , NOBRRN                                    -- BR連携No
                                        , NPBRRH                                    -- BR連携品目行No
                                        , TXBR01                                    -- 設計変更通知番号
                                        , TXBR02                                    -- 区分
                                        , TXBR03                                    -- コード番号
                                        , TXBR04                                    -- 名称
                                        , TXBR05                                    -- 機種
                                        , TXBR06                                    -- 新品目番号
                                        , TXBR07                                    -- 旧品目番号
                                        , TXBR08                                    -- 新図面番号
                                        , TXBR09                                    -- 旧図面番号
                                        , TXBR10                                    -- 理由
                                        , TXBR11                                    -- 改訂連絡書No.
                                        , TXBR12                                    -- フィードバックシー
                                        , TXBR13                                    -- 項目
                                        , TXBR14                                    -- BOM構成変更
                                        , TXBR15                                    -- 加工変更(MC含む
                                        , TXBR16                                    -- 認定仕様範囲の再確
                                        , TXBR17                                    -- 設計図書差し替え
                                        , TXBR18                                    -- 仕掛品(H/B/S/A/KK
                                        , TXBR19                                    -- 変更理由及び変更内
                                        , TXBR20                                    -- 作成者
                                        , TXBR21                                    -- 作成日
                                        , TXBR22                                    -- 検図者
                                        , TXBR23                                    -- 検図日
                                        , TXBR24                                    -- 承認者
                                        , TXBR25                                    -- 承認日
                                    ) 
                                    VALUES ( 
                                          ?                                   -- 削除フラグ
                                        , ?                                   -- 登録担当者コード
                                        , ?                                   -- 登録日時
                                        , ?                                   -- 登録プログラムID
                                        , ?                                   -- 更新担当者コード
                                        , ?                                   -- 更新日時
                                        , ?                                   -- 更新プログラムID
                                        , ?                                   -- BR連携No
                                        , ?                                   -- BR連携品目行No
                                        , ?                                   -- 設計変更通知番号
                                        , ?                                   -- 区分
                                        , ?                                   -- コード番号
                                        , ?                                   -- 名称
                                        , ?                                   -- 機種
                                        , ?                                   -- 新品目番号
                                        , ?                                   -- 旧品目番号
                                        , ?                                   -- 新図面番号
                                        , ?                                   -- 旧図面番号
                                        , ?                                   -- 理由
                                        , ?                                   -- 改訂連絡書No.
                                        , ?                                   -- フィードバックシー
                                        , ?                                   -- 項目
                                        , ?                                   -- BOM構成変更
                                        , ?                                   -- 加工変更(MC含む
                                        , ?                                   -- 認定仕様範囲の再確
                                        , ?                                   -- 設計図書差し替え
                                        , ?                                   -- 仕掛品(H/B/S/A/KK
                                        , ?                                   -- 変更理由及び変更内
                                        , ?                                   -- 作成者
                                        , ?                                   -- 作成日
                                        , ?                                   -- 検図者
                                        , ?                                   -- 検図日
                                        , ?                                   -- 承認者
                                        , ?                                   -- 承認日
                                    )";

                using (OdbcCommand command = new OdbcCommand(detailQuery, connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", this.DeleteFlag);
                    command.Parameters.AddWithValue("@SPIUSR", this.RegisteredUserCode);
                    command.Parameters.AddWithValue("@SPIDTM", this.RegisteredDateTime);
                    command.Parameters.AddWithValue("@SPIPGM", this.RegisteredProgramID);
                    command.Parameters.AddWithValue("@SPUUSR", this.UpdatedUserCode);
                    command.Parameters.AddWithValue("@SPUDTM", this.UpdatedDateTime);
                    command.Parameters.AddWithValue("@SPUPGM", this.UpdatedProgramID);
                    command.Parameters.AddWithValue("@NOBRRN", this.BaseRightNo);
                    command.Parameters.AddWithValue("@NPBRRH", this.ItemLineNo);
                    command.Parameters.AddWithValue("@TXBR01", this.NotificationNumber);
                    command.Parameters.AddWithValue("@TXBR02", this.Classification);
                    command.Parameters.AddWithValue("@TXBR03", this.CodeNumber);
                    command.Parameters.AddWithValue("@TXBR04", this.Name);
                    command.Parameters.AddWithValue("@TXBR05", this.Model);
                    command.Parameters.AddWithValue("@TXBR06", this.NewItemNumber);
                    command.Parameters.AddWithValue("@TXBR07", this.OldItemNumber);
                    command.Parameters.AddWithValue("@TXBR08", this.NewDrawingNumber);
                    command.Parameters.AddWithValue("@TXBR09", this.OldDrawingNumber);
                    command.Parameters.AddWithValue("@TXBR10", this.Reason);
                    command.Parameters.AddWithValue("@TXBR11", this.RevisionContactNo);
                    command.Parameters.AddWithValue("@TXBR12", this.FeedbackSheetNo);
                    command.Parameters.AddWithValue("@TXBR13", this.Item);
                    command.Parameters.AddWithValue("@TXBR14", this.BOMChange);
                    command.Parameters.AddWithValue("@TXBR15", this.ProcessingChange);
                    command.Parameters.AddWithValue("@TXBR16", this.SpecificationReview);
                    command.Parameters.AddWithValue("@TXBR17", this.DocumentReplacement);
                    command.Parameters.AddWithValue("@TXBR18", this.StockHandling);
                    command.Parameters.AddWithValue("@TXBR19", this.ChangeReasonAndContent);
                    command.Parameters.AddWithValue("@TXBR20", this.Creator);
                    command.Parameters.AddWithValue("@TXBR21", this.CreationDate);
                    command.Parameters.AddWithValue("@TXBR22", this.Checker);
                    command.Parameters.AddWithValue("@TXBR23", this.CheckDate);
                    command.Parameters.AddWithValue("@TXBR24", this.Approver);
                    command.Parameters.AddWithValue("@TXBR25", this.ApprovalDate);
                    // Execute the Insert command for detail
                    command.ExecuteNonQuery();
                }
            }
        }

        public static class IntegrationStatus
        {
            // *********************************************************************************
            // * プログラムの戻り値設定
            // *********************************************************************************
            // Publicなプロパティの定義
            public static int IntegrationErrorStatus { get; set; }
            public static string afterIntegrationComment_Item { get; set; }
            public static string afterIntegrationComment_ItemStructure { get; set; }
            public static string afterIntegrationComment_DesignChange { get; set; }

            // 戻り値を返すメソッド
            public static int GetReturnValue()
            {
                switch (IntegrationErrorStatus)
                {
                    case 1: // 正常
                        return 0;
                    case 9: // エラー
                        if (afterIntegrationComment_Item == null &&
                            afterIntegrationComment_ItemStructure == null &&
                            afterIntegrationComment_DesignChange == null)
                        {
                            return 3;
                        }
                        else if (afterIntegrationComment_Item != null &&
                                 afterIntegrationComment_ItemStructure == null &&
                                 afterIntegrationComment_DesignChange == null)
                        {
                            return 1;
                        }
                        else if (afterIntegrationComment_ItemStructure != null &&
                                 afterIntegrationComment_DesignChange == null)
                        {
                            return 2;
                        }
                        else if (afterIntegrationComment_ItemStructure == null &&
                                 afterIntegrationComment_DesignChange != null)
                        {
                            return 6;
                        }
                        else if (afterIntegrationComment_Item != null &&
                                 afterIntegrationComment_DesignChange != null)
                        {
                            return 7;
                        }
                        else if (afterIntegrationComment_ItemStructure != null &&
                                 afterIntegrationComment_Item != null)
                        {
                            return 5;
                        }
                        return 4;
                    default:
                        return 3; // 他のケース
                }
            }
        }

        public static class IdNumbering
        {
            // *********************************************************************************
            // * 採番クラス
            // *********************************************************************************
            public static int UpdateCMSABNV99(
                string registrationUserID,
                long integrationDateTime,
                string registrationProgramId,
                OdbcConnection connection)
            {
                // 引数が適切か確認
                if (connection == null || string.IsNullOrWhiteSpace(registrationUserID) ||
                    string.IsNullOrWhiteSpace(registrationProgramId))
                {
                    throw new ArgumentException("Invalid arguments provided.");
                }

                int updatedNoSeq; // 更新後のNOSEQ
                string selectQuery = "SELECT NOSEQ FROM CMSABNV99 WHERE CMSKBT = 'NOBRRN' FOR UPDATE WITH RS";
                string updateQuery = "UPDATE CMSABNV99 SET NOSEQ = ?, SPUUSR = ?, SPUDTM = ?, SPUPGM = ? WHERE CMSKBT = 'NOBRRN'";

                using (var transaction = connection.BeginTransaction())
                {
                    try
                    {
                        // リードロックをかけてデータを取得
                        Console.WriteLine("採番マスタのセレクト直前");
                        using (var selectCommand = new OdbcCommand(selectQuery, connection, transaction))
                        {
                            // 取得したデータを読み込む
                            using (var reader = selectCommand.ExecuteReader())
                            {
                                Console.WriteLine("採番マスタのセレクト直後");
                                if (reader.Read())
                                {
                                    updatedNoSeq = reader.GetInt32(0) + 1; // NOSEQを1加算
                                }
                                else
                                {
                                    throw new InvalidOperationException("No record found with CMSKBT = 'NOBRRN'.");
                                }
                            }
                        }

                        // 更新処理
                        Console.WriteLine("採番マスタの更新直前");
                        using (var updateCommand = new OdbcCommand(updateQuery, connection, transaction))
                        {
                            updateCommand.Parameters.AddWithValue("?", updatedNoSeq);
                            updateCommand.Parameters.AddWithValue("?", registrationUserID);
                            updateCommand.Parameters.AddWithValue("?", integrationDateTime);
                            updateCommand.Parameters.AddWithValue("?", registrationProgramId);

                            int rowsAffected = updateCommand.ExecuteNonQuery();
                            Console.WriteLine("採番マスタの更新直後");
                            if (rowsAffected == 0)
                            {
                                throw new InvalidOperationException("No record was updated.");
                            }
                        }

                        // コミット
                        transaction.Commit();
                    }
                    catch (Exception)
                    {
                        // ロールバック
                        transaction.Rollback();
                        throw;
                    }
                }
                return updatedNoSeq; // 更新後のNOSEQを戻り値として返す
            }
        }

        private static string GetTrimmedString(string input, int maxBytes)
        {
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(input); // UTF-8でバイト配列に変換
            if (bytes.Length <= maxBytes)
            {
                return input; // バイト数が256以下ならそのまま返す
            }

            // 256バイトに収めるために適切な文字数を探す
            int byteCount = 0;
            int charCount = 0;

            while (byteCount < maxBytes && charCount < input.Length)
            {
                byteCount += System.Text.Encoding.UTF8.GetByteCount(input[charCount].ToString());
                charCount++;
            }

            return input.Substring(0, charCount - 1); // 最後にオーバーした1文字を除外して返す
        }
        public static string[] SplitCsvLine(string line)
        {
            var result = new List<string>();
            var sb = new StringBuilder();
            bool inQuote = false;

            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];

                if (c == '"')
                {
                    if (inQuote && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        // 連続した "" → " として扱う
                        sb.Append('"');
                        i++;
                    }
                    else
                    {
                        // クォートの開始/終了
                        inQuote = !inQuote;
                    }
                }
                else if (c == ',' && !inQuote)
                {
                    // カンマ区切り（クォート外のみ）
                    result.Add(sb.ToString());
                    sb.Clear();
                }
                else
                {
                    sb.Append(c);
                }
            }

            result.Add(sb.ToString());
            return result.ToArray();
        }
    }
}
