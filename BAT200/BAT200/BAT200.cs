using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Data.Odbc;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Input;
using static BAT200.BAT200.ExecutionBudgetDetail;
using static BAT200.BAT200.ExecutionBudgetHeader;

namespace BAT200
{
    internal class BAT200
    {
        static int Main(string[] args)
        {
            // *********************************************************************************
            // * Mainルーチン
            // *********************************************************************************
            Console.WriteLine("プログラム開始しました。");
#if DEBUG
            Console.WriteLine("プログラム中断中です。再開するには何かキーを押下してださい。");
            string RTN = Console.ReadLine();
#endif

            try
            {
                // *********************************************************************************
                // * 変数定義
                // *********************************************************************************
                #region 変数定義
                String registrationProgramId = "BAT200";
                String registrationUserID = "BATCH";
                // 現在日時を取得し、yyyyMMddHHmmss形式で保持
                Decimal integrationDateTime = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmss"));

                //// CSVファイル名
                //string csvFileName_EBOM = null; 

                // 

                // 連携ステータスコメント
                string integrationStatusComment_Success = null; // (正常終了時)
                string integrationStatusComment_Error = null; // (エラー時)

                // フォルダパス
                string successFolderPath = null; // (正常終了用)
                string errorFolderPath = null; // (エラー時用)

                // 連携エラー区分
                bool integrationErrorType_EBOM = false; 

                // 連携ステータス
                String IntegrationErrorSatus = null;

                // 連携元パス
                string sourcePath_EBOM = null; 

                // 連携後フォルダパス（正常終了用）
                string successFolderPath_After_EBOM = null;

                // 連携後フォルダパス（エラー時用）
                string errorFolderPath_After_EBOM = null; 

                // 連携元パス（\をバックスラッシュに置き換え）
                string targetFolderEBOM = null; 

                // 連携後フォルダパス（正常終了用）（\をバックスラッシュに置き換え）
                string successTargetFolderEBOM = null; 

                // 連携後フォルダパス（エラー時用）（\をバックスラッシュに置き換え）
                string errorTargetFolderEBOM = null; 

                // 連携後コメント
                string afterIntegrationComment_EBOM = null; 

                // 不要パラメータ用
                string unnecessary = null;

                // ファイル名プリフィックス
                string filePrefix_EBOM = "tehaikousei";

                // 表示順見直し用の総親品目構成固有IDを退避するエリア
                string GetRootParentItemStructureUniqueID = null;

                //
                //HierarchyItem hierarchy = new HierarchyItem();

                #endregion
                // *********************************************************************************
                // * 引数のチェック
                // *********************************************************************************
                #region 引数のチェック
                if (args.Length != 1)
                {
#if DEBUG
                    Console.WriteLine("エラー: まとめ手配指示番号が指定されていません。");
                    Console.WriteLine("戻り値:9");
                    return 91;
#else
                    Environment.Exit(3); // 戻り値として「3」を返して終了
#endif
                }
                string strIraiNo = args[0];
                // まとめ手配指示番号の形式をバリデーション
                string pattern = @"^\d{10}$"; // 正規表現パターン
                if (!Regex.IsMatch(strIraiNo, pattern))
                {
#if DEBUG
                    Console.WriteLine("エラー: まとめ手配指示番号は9999999999形式である必要があります。");
                    Console.WriteLine("戻り値:9");
                    return 92;
#else
                    Environment.Exit(3); // 戻り値として「3」を返して終了
#endif
                }

                // まとめ手配指示番号出力
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
                    //GetMKUBN(connection, query, envMode, "KBBRRN", "3",
                    //         out sourcePath_EBOM, out unnecessary);
                    sourcePath_EBOM = "C:\\AS400CLIENTBATCH\\02ClientData\\01OutData\\01TEHAI";

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
                    string fixedPath = sourcePath_EBOM
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    targetFolderEBOM = Path.GetFullPath(fixedPath);

                    // 連携後フォルダパスの設定
                    successFolderPath_After_EBOM = sourcePath_EBOM + successFolderPath;

                    fixedPath = successFolderPath_After_EBOM
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    successTargetFolderEBOM = Path.GetFullPath(fixedPath);

                    errorFolderPath_After_EBOM = sourcePath_EBOM + errorFolderPath;

                    fixedPath = errorFolderPath_After_EBOM
                        .Replace('¥', '\\')   // 165 → 92
                        .Replace('/', '\\');  // スラッシュも許容する場合
                    errorTargetFolderEBOM = Path.GetFullPath(fixedPath);

                    // デバッグ出力
                    Console.WriteLine($"連携元パス: {sourcePath_EBOM}");
                    Console.WriteLine($"成功フォルダパス: {successFolderPath_After_EBOM}");
                    Console.WriteLine($"エラーフォルダパス: {errorFolderPath_After_EBOM}");
                    //
                    Console.WriteLine($"正常終了時コメント: {integrationStatusComment_Success}");
                    Console.WriteLine($"エラー終了時コメント: {integrationStatusComment_Error}");

                    #endregion
                    // *********************************************************************************
                    // * バリデーションチェック
                    // *********************************************************************************
                    // *********************************************************************************
                    // * E-BOM連携ファイルバリデーションチェック
                    // *********************************************************************************

                    #region E-BOM連携ファイルバリデーションチェック
                    var filesEBOM = Directory.GetFiles(targetFolderEBOM, "*.*", SearchOption.TopDirectoryOnly);
                    Console.WriteLine($"ターゲットフォルダ: {targetFolderEBOM}");
                    var allFiles = Directory.GetFiles(targetFolderEBOM, "*.*").ToList();
                    Console.WriteLine($"フォルダ内のファイル数: {allFiles.Count}");
                    foreach (var file in allFiles)
                    {
                        Console.WriteLine($"見つかったファイル: {file}");
                    }

                    // 衝突しないファイルリストの出力
                    foreach (var file in filesEBOM)
                    {
                        Console.WriteLine($"フィルタ後のファイル: {file}");
                    }
                    if (filesEBOM.Any())
                    {
                        Console.WriteLine("ファイルが見つかりました。");
                    }
                    else
                    {
                        Console.WriteLine("ファイルが見つかりませんでした。");
                    }

                    foreach (var file in filesEBOM)
                    {
                        Console.WriteLine($"ファイルを読み込んでいます: {file}");
                        //int lineCount = 0;
                        //using (StreamReader reader = new StreamReader(file, Encoding.GetEncoding("Shift_JIS")))
                        //{
                        //    while (!reader.EndOfStream)
                        //    {
                        //        var line = reader.ReadLine();
                        //        lineCount++;
                        //        Console.WriteLine($"行 {lineCount}: {line}"); // 行の内容を出力
                        //    }
                        //}
                    }







                    //var filesEBOM = Directory.GetFiles(targetFolderEBOM, "*.*", SearchOption.TopDirectoryOnly)
                    filesEBOM.Where(f =>
                               Path.GetFileName(f).Contains(filePrefix_EBOM) &&
                               Path.GetFileName(f).Contains(strIraiNo));

                    if (filesEBOM.Count() > 1)
                    {
                        // ファイルが存在しないもしくは複数ある場合の措置
                        // 別途エラー処理を入れる。
                        integrationErrorType_EBOM = true;
                    }
                    foreach (var file in filesEBOM)
                    {
                        // 存在するファイル分だけループ
                        Console.WriteLine(file);
                        bool isHeader = true;
                        Decimal rowCount = 0;
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
                                columns[12] = columns[12].Replace("/", ""); // 手配依頼日はスラッシュを削除

                                // バリデーション実行
                                var errors = CsvValidator.Validate<EBOMRecord>(columns);

                                if (errors.Any())
                                {
                                    //
                                    Console.WriteLine($"E-BOM連携ファイルの{rowCount}行目でエラーがあります：");
                                    errors.ForEach(e => Console.WriteLine("  - " + e));
                                    //
                                    afterIntegrationComment_EBOM += $"{rowCount}行目:";
                                    errors.ForEach(e => afterIntegrationComment_EBOM += $"  - {e}" + integrationStatusComment_Success);
                                    integrationErrorType_EBOM = true;
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
                    // * E-BOM連携ファイルバリデーションチェック終了
                    // *********************************************************************************
                    // *********************************************************************************
                    // * バリデーションチェック終了
                    // *********************************************************************************
                    if (integrationErrorType_EBOM)
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
                                // * 配列 WK手配完了の実体化
                                // *********************************************************************************
                                UniqueCodeArray uniqueCodes = new UniqueCodeArray();
                                BuildOutStructureKey buildOutStructureKey = new BuildOutStructureKey(); 
                                // *********************************************************************************
                                // * 実行予算明細ファイル(E-BOM)追加処理
                                // *********************************************************************************
                                #region 実行予算明細ファイル(E-BOM)に追加

                                foreach (var file in filesEBOM)
                                {
                                    // 存在するファイル分だけループ
                                    Console.WriteLine(file);
                                    bool isHeader = true;
                                    Decimal rowCount = 0;



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
                                            columns[12] = columns[12].Replace("/", "");　// 手配依頼日はスラッシュを削除



                                            // E-BOM連携ファイルをオブジェクト「EBOMRecord」に設定
                                            var EBOMRecord = new EBOMRecord
                                            {
                                                CodeNumber = columns[0],    //	コード番号
                                                CodeBranchNumber = columns[1],  //	コード番号枝番
                                                ParentItemNumber = columns[2],  //	親品目番号
                                                BalloonNumber =columns[3],  //	風船番号(照番)
                                                Order = columns[4],  //	順序
                                                ChildItemNumber = columns[5],   //	子品目番号
                                                PFlag = columns[6], //	Pフラグ
                                                MFlag = columns[7], //	Mフラグ
                                                Quantity = columns[8],   //	員数
                                                OrderQuantity = columns[9],  //	手配数量
                                                OrderInstructionNumber = columns[10],   //	手配指示番号
                                                SummaryOrderInstructionNumber = columns[11],    //	まとめ手配指示番号
                                                OrderRequestDate = columns[12],    //	手配依頼日
                                                OrderItemFlag = columns[13],    //	手配品目フラグ
                                                OrderCompletionFlag = columns[14],  //	手配完了フラグ
                                                ParentItemUniqueID = columns[15],   //	親品目構成固有ID
                                                ItemUniqueID = columns[16], //	品目構成固有ID
                                                HierarchyLevel = columns[17],    //	階層レベル
                                                ItemNumber1 = columns[18],  //	品目番号1
                                                ItemNumber2 = columns[19],  //	品目番号2
                                                ItemNumber3 = columns[20],  //	品目番号3
                                                ItemNumber4 = columns[21],  //	品目番号4
                                                ItemNumber5 = columns[22],  //	品目番号5
                                                ItemNumber6 = columns[23],  //	品目番号6
                                                ItemNumber7 = columns[24],  //	品目番号7
                                                ItemNumber8 = columns[25],  //	品目番号8
                                                ItemNumber9 = columns[26],  //	品目番号9
                                                ItemNumber10 = columns[27], //	品目番号10
                                                ExternalInspection = columns[28],   //	外部検査
                                                HighPressureGasCertified = columns[29], //	高圧ガス認定品
                                                FourTimesPressureResistance = columns[30],  //	4倍耐圧
                                                WoodPattern = columns[31],  //	木型
                                                PaintProcedureExist = columns[32],  //	塗装要領書有無
                                                PaintingNotAllowedAtGasConnection = columns[33],    //	接ガス部塗装不可
                                                OilRestriction = columns[34],   //	禁油処理
                                                WaterRestriction = columns[35], //	禁水処理
                                                ForNuclearUse = columns[36],    //	原子力用
                                                DesignChangeNotificationNumber = columns[37],   //	設計変更通知番号
                                                AssemblyDivision = columns[38], //	組立区分
                                                Remarks = columns[39],  //	備考
                                                ManufacturerCode = columns[40], //	メーカーコード
                                                RequirementForStandard2_02 = columns[41],   //	準2-02対応要否
                                                BudgetNumber = columns[42], //	予算書番号
                                                StrengthCalculationExist = columns[43], //	強度計算書有無
                                                PreDeliveryDocumentation = columns[44], //	調達品の事前納品図書
                                                CompletionDocumentation = columns[45],  //	調達品の完成時納入図書
                                                ItemUniqueAttribute19 = columns[46],    //	品目構成固有属性19
                                                ItemUniqueAttribute20 = columns[47],	//	品目構成固有属性20
                                            };
                                            // 実行予算見出ファイルのエンティティクラスを実体化
                                            var ExecutionBudgetHeader = new ExecutionBudgetHeader(connection, transaction);
                                            // 実行予算見出ファイルの取得
                                            ExBudgetHead ExBudgetHead = ExecutionBudgetHeader.GetExBudgetHDByID(EBOMRecord.CodeNumber, EBOMRecord.CodeBranchNumber);

                                            if (ExBudgetHead != null)
                                            {
                                                // 実行予算見出ファイルが取得できたとき、
                                                // 実行予算明細ファイルを取得する。
                                                // 実行予算明細ファイルのエンティティクラスを実体化
                                                var ExecutionBudgetDetail = new ExecutionBudgetDetail(connection, transaction);
                                                //BOM行Noの最大値を取得
                                                int maxBomLineNo = ExecutionBudgetDetail.GetMaxBomLineNo(EBOMRecord.CodeNumber, EBOMRecord.CodeBranchNumber, ExBudgetHead.Revision);
                                                //

                                                // 実行予算明細ファイルの取得
                                                var ExBudgetDetail = ExecutionBudgetDetail.GetExBudgetDTByID(EBOMRecord.CodeNumber, EBOMRecord.CodeBranchNumber, EBOMRecord.ItemUniqueID);
                                                if (ExBudgetDetail != null)
                                                {
                                                    // 実行予算明細ファイルの取得できた場合は、エラーとする。
                                                    Console.WriteLine($"エラーです：実行予算明細ファイル:{EBOMRecord.CodeNumber} {EBOMRecord.CodeBranchNumber} {EBOMRecord.ItemUniqueID} が既に存在します。");
                                                    // 実行予算明細が取得できた場合
                                                    integrationErrorType_EBOM = true;
                                                    afterIntegrationComment_EBOM += $"{rowCount}行目 既に実行予算明細が存在します{EBOMRecord.CodeNumber} {EBOMRecord.CodeBranchNumber} {EBOMRecord.ItemUniqueID}";
                                                }
                                                else
                                                {
                                                    // BOM行Noを設定
                                                    int newBomLineNo = maxBomLineNo + 1; // 最大行番号に1加算
                                                    // 実行予算見出ファイルが存在しない場合追加処理を行う
                                                    Console.WriteLine($"実行予算明細ファイル:{EBOMRecord.CodeNumber} {EBOMRecord.CodeBranchNumber} {EBOMRecord.ItemUniqueID}を追加します");

                                                    // 表示順の見直し対象とする、コード番号、枝番、実行予算リビジョンを退避しておく
                                                    buildOutStructureKey.AddCode(ExBudgetHead.CodeNumber, ExBudgetHead.BranchNumber, ExBudgetHead.Revision);
                                                    // 手配完了時、WK手配完了配列に退避
                                                    //
                                                    if (EBOMRecord.OrderCompletionFlag == "1")
                                                    {
                                                        uniqueCodes.AddCode(ExBudgetHead.CodeNumber, ExBudgetHead.BranchNumber, ExBudgetHead.Revision, EBOMRecord.AssemblyDivision);
                                                    }
                                                    //
                                                    // 品目マスタエンティティクラスを実体化
                                                    var ItemMaster = new ItemMasterRepository(connection, transaction);
                                                    var Item = ItemMaster.GetItemById(EBOMRecord.ChildItemNumber);
                                                    if (Item != null)
                                                    {
                                                        //品目マスタ取得時のみ処理
                                                        //
                                                        ExBudgetDetail ExBudgetDetail2 = new ExBudgetDetail();
                                                        ExBudgetDetail2.DeleteFlag = Convert.ToInt32(0);                                                 // 削除フラグ (0: 削除されていない, 1: 削除された)
                                                        ExBudgetDetail2.RegisteredUserCode = registrationUserID;                                         // 登録担当者コード (最大5文字)
                                                        ExBudgetDetail2.RegisteredDateTime = Convert.ToDecimal(integrationDateTime);                       // 登録日時 (数値表現)
                                                        ExBudgetDetail2.RegisteredProgramID = registrationProgramId;                                     // 登録プログラムID (最大50文字)
                                                        ExBudgetDetail2.UpdatedUserCode = registrationUserID;                                            // 更新担当者コード (最大5文字)
                                                        ExBudgetDetail2.UpdatedDateTime = Convert.ToDecimal(integrationDateTime);                          // 更新日時 (数値表現)
                                                        ExBudgetDetail2.UpdatedProgramID = registrationProgramId;                                        // 更新プログラムID (最大50文字)
                                                        ExBudgetDetail2.CodeNumber = ExBudgetHead.CodeNumber;                                            // コード番号 (主キー, 最大10文字)
                                                        ExBudgetDetail2.BranchNumber = ExBudgetHead.BranchNumber;                                        // 枝番 (主キー, 最大3文字)
                                                        ExBudgetDetail2.Revision = ExBudgetHead.Revision;                                                // 実行予算リビジョン (主キー)
                                                        ExBudgetDetail2.BomLineNo = Convert.ToDecimal(EBOMRecord.Order);                                   // BOM行No (主キー)
                                                        ExBudgetDetail2.DisplayLineNo = Convert.ToDecimal(EBOMRecord.Order);                               // 表示行No (数値表現)
                                                        ExBudgetDetail2.ArrangementInstructionNo = EBOMRecord.OrderInstructionNumber;                    // 手配指示番号 (最大25文字)
                                                        ExBudgetDetail2.SummaryArrangementInstructionNo = EBOMRecord.SummaryOrderInstructionNumber;      // まとめ手配指示番号 (最大25文字)
                                                        ExBudgetDetail2.DesignChangeNotificationNo = EBOMRecord.DesignChangeNotificationNumber;          // 設計変更通知番号 (最大9文字)
                                                        ExBudgetDetail2.DesignChangeDivision = string.Empty;                                             // 設変区分 (最大1文字)
                                                        ExBudgetDetail2.AssemblyDivision = EBOMRecord.AssemblyDivision;                                  // 組立区分 (最大10文字)
                                                        ExBudgetDetail2.HierarchyLevel = Convert.ToDecimal(EBOMRecord.HierarchyLevel);                     // 階層レベル (数値表現)
                                                        ExBudgetDetail2.ParentItemStructureUniqueID = EBOMRecord.ParentItemUniqueID;                     // 親品目構成固有ID (最大1024文字)
                                                        ExBudgetDetail2.ItemStructureUniqueID = EBOMRecord.ItemUniqueID;                                 // 品目構成固有ID (最大1024文字)
                                                        ExBudgetDetail2.BudgetBillNo = EBOMRecord.BudgetNumber;                                          // 予算書No (最大30文字)
                                                        ExBudgetDetail2.BalloonNumber = Convert.ToDecimal(EBOMRecord.BalloonNumber);                       // 風船番号 (数値表現)
                                                        ExBudgetDetail2.Sequence = Convert.ToDecimal(EBOMRecord.Order);                                    // 順序 (数値表現)
                                                        ExBudgetDetail2.ParentBomLineNo = Convert.ToDecimal(0);                                            // 親BOM行No (数値表現)
                                                        ExBudgetDetail2.ItemCode = EBOMRecord.ChildItemNumber;                                           // 品目コード (最大25文字)
                                                        ExBudgetDetail2.ItemName = Item.NMHINM;                                                          // 品目名 (最大66文字)
                                                        ExBudgetDetail2.Quantity = Convert.ToDecimal(EBOMRecord.Quantity);                                 // 員数 (数値表現)
                                                        ExBudgetDetail2.Amount = decimal.TryParse(EBOMRecord.OrderQuantity, out var temp) ? temp : 0;　　// 数量 (数値表現)
                                                        ExBudgetDetail2.ArrangementDivision = Item.KBTEHI;                                               // 手配区分 (最大3文字)
                                                        ExBudgetDetail2.ItemTypeDivision = Item.KBHNTY;                                                  // 品目タイプ区分 (最大2文字)
                                                        ExBudgetDetail2.UnitCode = Item.CDTANI;                                                          // 単位コード (最大3文字)
                                                        ExBudgetDetail2.Dimension1 = Convert.ToDecimal(Item.VLSUN1);                                       // 寸法1 (数値表現)
                                                        ExBudgetDetail2.Dimension2 = Convert.ToDecimal(Item.VLSUN2);                                       // 寸法2 (数値表現)
                                                        ExBudgetDetail2.Dimension3 = Convert.ToDecimal(Item.VLSUN3);                                       // 寸法3 (数値表現)
                                                        ExBudgetDetail2.DrawingNo = Item.VLZUBN;                                                         // 図番/購入仕様書 (最大30文字)
                                                        ExBudgetDetail2.DrawingRevision = Convert.ToDecimal(0);                                            // 図面リビジョン (数値表現)
                                                        ExBudgetDetail2.PaintColor = string.Empty;                                                       // 塗装色 (最大32文字)
                                                        ExBudgetDetail2.MaterialCode = Item.CDZAIS1;                                                     // 材質コード(最大8文字)
                                                        ExBudgetDetail2.Specification = string.Empty;                                                    // 仕様(最大62文字)
                                                        ExBudgetDetail2.MakerCode = EBOMRecord.ManufacturerCode;                                         // メーカーコード(最大8文字)
                                                        ExBudgetDetail2.LT = Convert.ToDecimal(0);                                                         // LT(数値表現)
                                                        ExBudgetDetail2.Remarks = EBOMRecord.Remarks;                                                    // 備考(最大44文字)
                                                        ExBudgetDetail2.PerformanceBookHandlingDivision = Item.KBSESK;                                   // 成績書扱い区分(最大1文字)
                                                        ExBudgetDetail2.PurchaseSpecificationExistence = string.Empty;                                   // 購入仕様書有無(最大1文字)
                                                        //ExBudgetDetail2.MillSheetRequirement = EBOMRecord.MFlag;                                         // ミルシート
                                                        ExBudgetDetail2.MillSheetRequirement = string.Empty;
                                                        //ExBudgetDetail2.StrengthCalculationSheetExistence = EBOMRecord.StrengthCalculationExist;         // 強度計算書有無 (最大1文字)
                                                        ExBudgetDetail2.StrengthCalculationSheetExistence = string.Empty;
                                                        //ExBudgetDetail2.ExternalInspection = EBOMRecord.ExternalInspection;                              // 外部検査(最大1文字)
                                                        ExBudgetDetail2.ExternalInspection = string.Empty;
                                                        ExBudgetDetail2.CertificationRequirement = Item.FGKENT;                                          // 検定要否(最大1文字)
                                                        ExBudgetDetail2.DrawingRequirement = Item.FGZUMN;                                                // 図面要否(最大1文字)
                                                        ExBudgetDetail2.SpecificationRequirement = Item.FGSHIY;                                          // 仕様書要否(最大1文字)
                                                        ExBudgetDetail2.HeatTreatmentRecordRequirement = Item.FGNETU;                                    // 熱処理記録要否(最大1文字)
                                                        ExBudgetDetail2.DimensionalConfirmationSheetRequirement = Item.FGSUNK;                           // 寸法確認書要否(最大1文字)
                                                        ExBudgetDetail2.HandlingInstructionSheetRequirement = Item.FGSETU;                               // 取扱説明書要否(最大1文字)
                                                        ExBudgetDetail2.KHKQualificationRequirement = Item.FGKHKG;                                       // KHK合格書要否(最大1文字)
                                                        ExBudgetDetail2.HighPressureGasCertificationRequirement = Item.FGKGSY;                           // 高圧ガス認定書要否(最大1文字)
                                                        //ExBudgetDetail2.ProcessManagementRequirement = EBOMRecord.PFlag;                                 // プロセス管理要否(最大1文字)
                                                        ExBudgetDetail2.ProcessManagementRequirement = string.Empty;
                                                        //ExBudgetDetail2.HighPressureGasCertifiedProduct = EBOMRecord.HighPressureGasCertified;           // 高圧ガス認定品 (最大1文字)
                                                        ExBudgetDetail2.HighPressureGasCertifiedProduct = string.Empty;
                                                        //ExBudgetDetail2.QuadruplePressureHot = EBOMRecord.FourTimesPressureResistance;                   // 4倍耐圧(最大1文字)
                                                        ExBudgetDetail2.QuadruplePressureHot = string.Empty;
                                                        //ExBudgetDetail2.WoodenType = EBOMRecord.WoodPattern;                                             // 木型(最大1文字)
                                                        ExBudgetDetail2.WoodenType = string.Empty;
                                                        //ExBudgetDetail2.CoatingInstructionsExistence = EBOMRecord.PaintProcedureExist;                   // 塗装要領書有無(最大1文字)
                                                        ExBudgetDetail2.CoatingInstructionsExistence = string.Empty;
                                                        //ExBudgetDetail2.GasConnectionPartCannotBeCoated = EBOMRecord.PaintingNotAllowedAtGasConnection;  // 接ガス部塗装不可(最大1文字)
                                                        ExBudgetDetail2.GasConnectionPartCannotBeCoated = string.Empty;
                                                        //ExBudgetDetail2.OilRestriction = EBOMRecord.OilRestriction;                                      // 禁油処理(最大3文字)
                                                        ExBudgetDetail2.OilRestriction = string.Empty;
                                                        //ExBudgetDetail2.WaterRestriction = EBOMRecord.WaterRestriction;                                  // 禁水処理(最大3文字)
                                                        ExBudgetDetail2.WaterRestriction = string.Empty;
                                                        //ExBudgetDetail2.NuclearUse = EBOMRecord.ForNuclearUse;                                           // 原子力用(最大1文字)
                                                        ExBudgetDetail2.NuclearUse = string.Empty;
                                                        //ExBudgetDetail2.PreparationForJun202 = EBOMRecord.RequirementForStandard2_02;                    // 準2-02対応要否(最大1文字)
                                                        ExBudgetDetail2.PreparationForJun202 = string.Empty;
                                                        ExBudgetDetail2.PressureTestRequirement = string.Empty;                                          // 耐圧試験要否(最大1文字)
                                                        ExBudgetDetail2.Others = string.Empty;                                                           //  その他 (最大256文字)
                                                        ExBudgetDetail2.SpecificationNo = string.Empty;                                                  // 仕様書No(最大20文字)
                                                        //ExBudgetDetail2.ItemCode1 = EBOMRecord.ItemNumber1;                                              // 品目コード1(最大25文字)
                                                        ExBudgetDetail2.ItemCode1 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode2 = EBOMRecord.ItemNumber2;                                              // 品目コード2(最大25文字)
                                                        ExBudgetDetail2.ItemCode2 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode3 = EBOMRecord.ItemNumber3;                                              // 品目コード3(最大25文字)
                                                        ExBudgetDetail2.ItemCode3 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode4 = EBOMRecord.ItemNumber4;                                              // 品目コード4(最大25文字)
                                                        ExBudgetDetail2.ItemCode4 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode5 = EBOMRecord.ItemNumber5;                                              // 品目コード5(最大25文字)
                                                        ExBudgetDetail2.ItemCode5 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode6 = EBOMRecord.ItemNumber6;                                              // 品目コード6(最大25文字)
                                                        ExBudgetDetail2.ItemCode6 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode7 = EBOMRecord.ItemNumber7;                                              // 品目コード7(最大25文字)
                                                        ExBudgetDetail2.ItemCode7 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode8 = EBOMRecord.ItemNumber8;                                              // 品目コード8(最大25文字)
                                                        ExBudgetDetail2.ItemCode8 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode9 = EBOMRecord.ItemNumber9;                                              // 品目コード9(最大25文字)
                                                        ExBudgetDetail2.ItemCode9 = string.Empty;
                                                        //ExBudgetDetail2.ItemCode10 = EBOMRecord.ItemNumber10;                                            // 品目コード10(最大25文字)
                                                        ExBudgetDetail2.ItemCode10 = string.Empty;
                                                        ExBudgetDetail2.PreDeliveryDiagram = EBOMRecord.PreDeliveryDocumentation;                        // 調達品の事前納品図(最大256文字)
                                                        ExBudgetDetail2.CompletedDelivery = EBOMRecord.CompletionDocumentation;                          // 調達品の完成時納入(最大256文字)
                                                        ExBudgetDetail2.UniqueAttribute19 = EBOMRecord.ItemUniqueAttribute19;                            // 製番固有属性19(最大256文字)
                                                        ExBudgetDetail2.UniqueAttribute20 = EBOMRecord.ItemUniqueAttribute20;                            // 製番固有属性20(最大256文字)
                                                        ExBudgetDetail2.ProcurementItemFlag = EBOMRecord.OrderItemFlag;                                  // 手配品目フラグ
                                                        //ExBudgetDetail2.ProcurementItemFlag = string.Empty;
                                                        ExBudgetDetail2.ProcurementCompletionFlag = EBOMRecord.OrderCompletionFlag;                      // 手配完了フラグ
                                                        //ExBudgetDetail2.ProcurementCompletionFlag = string.Empty;
                                                        ExBudgetDetail2.MBOMReflectionFlag = string.Empty;                                               // MBOM取込フラグ
                                                        ExBudgetDetail2.OriginalCodeNumber = string.Empty;                                               // 元コード番号(S-BOM)
                                                        ExBudgetDetail2.OriginalBranchNumber = string.Empty;                                             // 元枝番(S-BOM)
                                                        ExBudgetDetail2.Revision_S_BOM = Convert.ToDecimal(0);                                           // リビジョン(S-BOM)
                                                        ExBudgetDetail2.ItemUUID_S_BOM = string.Empty;                                                   // 品目UUID(S-BOM)
                                                        // 実行予算明細ファイルのエンティティクラスを実体化
                                                        var newExBudgetDetail = new ExBudgetDetail();
                                                        ExBudgetDetail2.BomLineNo = newBomLineNo; // 新しいBOM行番号を設定
                                                        ExecutionBudgetDetail.Insert(ExBudgetDetail2);
                                                    }
                                                    else
                                                    {
                                                        //　品目マスタ取得できない場合
                                                        integrationErrorType_EBOM = true;
                                                        afterIntegrationComment_EBOM += $"{rowCount}行目 品目マスタが抽出できませんでした {EBOMRecord.CodeNumber}";
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                // 実行予算見出しファイルの取得できない場合は、エラーとする。
                                                Console.WriteLine($"エラーです：実行予算見出しファイル:{EBOMRecord.CodeNumber} {EBOMRecord.CodeBranchNumber} が存在しません。");
                                                // 実行予算見出しファイルの取得できない場合
                                                integrationErrorType_EBOM = true;
                                                afterIntegrationComment_EBOM += $"{rowCount}行目 実行予算見出しファイルが存在しません {EBOMRecord.CodeNumber} {EBOMRecord.CodeBranchNumber}";
                                            }
                                        }
                                        // 存在するファイル分だけループの終了位置
                                    }
                                }
                                #endregion
                                // *********************************************************************************
                                // * 実行予算明細ファイル(E-BOM)の手配完了フラグの更新処理
                                // *********************************************************************************
                                #region 実行予算明細ファイル(E-BOM)の手配完了フラグの更新処理
                                var ExecutionBudgetDetail2 = new ExecutionBudgetDetail(connection, transaction);
                                foreach (var code in uniqueCodes.Codes)
                                {
                                    ExBudgetDetail ExBudgetDetail = new ExBudgetDetail();
                                    ExBudgetDetail.UpdatedUserCode = registrationUserID;
                                    ExBudgetDetail.UpdatedDateTime = Convert.ToDecimal(integrationDateTime);
                                    ExBudgetDetail.UpdatedProgramID = registrationProgramId;
                                    ExBudgetDetail.CodeNumber = code.CodeNumber;
                                    ExBudgetDetail.BranchNumber = code.CodeBranchNumber;
                                    ExBudgetDetail.Revision = code.Revision;
                                    ExBudgetDetail.AssemblyDivision = code.AssemblyDivision;
                                    ExecutionBudgetDetail2.Update(ExBudgetDetail);
                                }
                                #endregion
                                // *********************************************************************************
                                // * 表示順の見直し処理（構成展開実施）
                                // *********************************************************************************
                                #region 表示順の見直し処理（構成展開実施）
                                var ExecutionBudgetDetail3 = new ExecutionBudgetDetail(connection, transaction);

                                foreach ( var code in buildOutStructureKey.Codes)
                                {
                                    // *********************************************************************************
                                    // * 退避エリアのキーの分だけ繰り返し
                                    // *********************************************************************************
                                    GetRootParentItemStructureUniqueID = ExecutionBudgetDetail3.GetRootParentItemStructureUniqueID(code.CodeNumber, code.CodeBranchNumber,code.Revision );
                                    if (GetRootParentItemStructureUniqueID == null)
                                    {
                                        Console.WriteLine($"コードNo:{code.CodeNumber} 枝番:{code.CodeBranchNumber} 実行予算リビジョン:{code.Revision}について、");
                                        Console.WriteLine("実行予算明細ファイルに総親品目構成固有IDが存在しません。");
                                        Console.WriteLine("表示行Noの見直しはされません。");
                                    }
                                    else
                                    {
                                        // 構成展開処理実施
                                        decimal displayLineNo = 0;
                                        var hierarchy = GetHierarchy(connection, transaction, GetRootParentItemStructureUniqueID);
                                        // 結果を表示
                                        foreach (var item in hierarchy)
                                        {
                                            displayLineNo++;
                                            Console.WriteLine($"親品目構成固有ID: {item.ParentID}, 品目構成固有ID: {item.ChildID}, レベル: {item.Level}");
                                            ExBudgetDetail ExBudgetDetail = new ExBudgetDetail();
                                            ExBudgetDetail.UpdatedUserCode = registrationUserID;
                                            ExBudgetDetail.UpdatedDateTime = Convert.ToDecimal(integrationDateTime);
                                            ExBudgetDetail.UpdatedProgramID = registrationProgramId;
                                            ExBudgetDetail.CodeNumber = code.CodeNumber;
                                            ExBudgetDetail.BranchNumber = code.CodeBranchNumber;
                                            ExBudgetDetail.Revision = code.Revision;
                                            ExBudgetDetail.ParentItemStructureUniqueID = item.ParentID;
                                            ExBudgetDetail.ItemStructureUniqueID = item.ChildID;
                                            ExBudgetDetail.DisplayLineNo = displayLineNo;
                                            ExecutionBudgetDetail3.DiplaySEQUpdate(ExBudgetDetail);
                                        }
                                    }
                                }
                                #endregion
                                // *********************************************************************************
                                // * コミット実施全てOKのときのみ
                                // *********************************************************************************

                                if (!integrationErrorType_EBOM)
                                {
                                    transaction.Commit();
                                }
                            }
                            catch (Exception ex)
                            {
                                // エラーが発生した場合ロールバック
                                transaction.Rollback();
                                // エラー状態を設定
                                integrationErrorType_EBOM = true;
                            }
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
                    if (integrationErrorType_EBOM)
                    {
                        IntegrationErrorSatus = "9"; // エラー
                    }
                    else
                    {
                        IntegrationErrorSatus = "1"; //正常
                    }
                    // *********************************************************************************
                    // * BaseRight連携履歴（E-BOM）
                    // *********************************************************************************
                    #region BaseRight連携履歴（E-BOM）
                    foreach (var file in filesEBOM)
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
                            BaseRightCategory = "3",                                        // 3:E-BOM
                            LinkedDate = Convert.ToInt64(integrationDateTime),
                            Sequence = 1,
                            Status = integrationErrorType_EBOM is true
                                        ? "9"
                                        : "1",
                            Comment = afterIntegrationComment_EBOM != null
                                      ? GetTrimmedString(afterIntegrationComment_EBOM, Math.Min(Encoding.UTF8.GetByteCount(afterIntegrationComment_EBOM), 256))
                                      : string.Empty,                            
                            SourceFilePath = sourcePath_EBOM != null
                                      ? GetTrimmedString(sourcePath_EBOM, Math.Min(Encoding.UTF8.GetByteCount(sourcePath_EBOM), 256))
                                      : string.Empty,
                            DestinationFilePath = integrationErrorType_EBOM is true
                                      ? GetTrimmedString(errorFolderPath_After_EBOM, Math.Min(Encoding.UTF8.GetByteCount(errorFolderPath_After_EBOM), 256))
                                      : GetTrimmedString(successFolderPath_After_EBOM, Math.Min(Encoding.UTF8.GetByteCount(successFolderPath_After_EBOM), 256))
                        };
                        bool isHeader = true;
                        Decimal rowCount = 0;
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
                                header.Details.Add(new BaseRightHistoryDetailEBOM
                                {
                                    DeleteFlag = 0,                                                 // 削除フラグ
                                    RegisteredUserCode = registrationUserID,                        // 登録担当者コード
                                    RegisteredDateTime = Convert.ToInt64(integrationDateTime),      // 登録日時
                                    RegisteredProgramID = registrationProgramId,                    // 登録プログラムID
                                    UpdatedUserCode = registrationUserID,                           // 更新担当者コード
                                    UpdatedDateTime = Convert.ToInt64(integrationDateTime),         // 更新日時
                                    UpdatedProgramID = registrationProgramId,                       // 更新プログラムID
                                    BaseRightNo = BRNo.ToString("D10"),                             // BR連携No
                                    EBOMLineNo = Convert.ToInt32(rowCount),                         // BR連携品目行No
                                    CodeNumber = columns.Length > 1 && columns[0] != null
                                     ? columns[0].Substring(0, Math.Min(columns[0].Length, 512))
                                     : string.Empty,                                                                                      //コード番号
                                    CodeBranchNumber = columns.Length > 2 && columns[1] != null
                                     ? columns[1].Substring(0, Math.Min(columns[1].Length, 512))
                                     : string.Empty,                                                                                      //コード番号枝番
                                    ParentItemNumber = columns.Length > 3 && columns[2] != null
                                     ? columns[2].Substring(0, Math.Min(columns[2].Length, 512))
                                     : string.Empty,                                                                                      //親品目番号
                                    BalloonNumber = columns.Length > 4 && columns[3] != null
                                     ? columns[3].Substring(0, Math.Min(columns[3].Length, 512))
                                     : string.Empty,                                                                                      //風船番号(照番)
                                    Order = columns.Length > 5 && columns[4] != null
                                     ? columns[4].Substring(0, Math.Min(columns[4].Length, 512))
                                     : string.Empty,                                                                                      //順序
                                    ChildItemNumber = columns.Length > 6 && columns[5] != null
                                     ? columns[5].Substring(0, Math.Min(columns[5].Length, 512))
                                     : string.Empty,                                                                                      //子品目番号
                                    PFlag = columns.Length > 7 && columns[6] != null
                                     ? columns[6].Substring(0, Math.Min(columns[6].Length, 512))
                                     : string.Empty,                                                                                      //Pフラグ
                                    MFlag = columns.Length > 8 && columns[7] != null
                                     ? columns[7].Substring(0, Math.Min(columns[7].Length, 512))
                                     : string.Empty,                                                                                      //Mフラグ
                                    Quantity = columns.Length > 9 && columns[8] != null
                                     ? columns[8].Substring(0, Math.Min(columns[8].Length, 512))
                                     : string.Empty,                                                                                      //員数
                                    OrderQuantity = columns.Length > 10 && columns[9] != null
                                     ? columns[9].Substring(0, Math.Min(columns[9].Length, 512))
                                     : string.Empty,                                                                                      //手配数量
                                    OrderInstructionNumber = columns.Length > 11 && columns[10] != null
                                     ? columns[10].Substring(0, Math.Min(columns[10].Length, 512))
                                     : string.Empty,                                                                                      //手配指示番号
                                    SummaryOrderInstructionNumber = columns.Length > 12 && columns[11] != null
                                     ? columns[11].Substring(0, Math.Min(columns[11].Length, 512))
                                     : string.Empty,                                                                                      //まとめ手配指示番号
                                    OrderRequestDate = columns.Length > 13 && columns[12] != null
                                     ? columns[12].Substring(0, Math.Min(columns[12].Length, 512))
                                     : string.Empty,                                                                                      //手配依頼日
                                    OrderItemFlag = columns.Length > 14 && columns[13] != null
                                     ? columns[13].Substring(0, Math.Min(columns[13].Length, 512))
                                     : string.Empty,                                                                                      //手配品目フラグ
                                    OrderCompletionFlag = columns.Length > 15 && columns[14] != null
                                     ? columns[14].Substring(0, Math.Min(columns[14].Length, 512))
                                     : string.Empty,                                                                                      //手配完了フラグ
                                    ParentItemUniqueID = columns.Length > 16 && columns[15] != null
                                     ? columns[15].Substring(0, Math.Min(columns[15].Length, 512))
                                     : string.Empty,                                                                                      //親品目構成固有ID
                                    ItemUniqueID = columns.Length > 17 && columns[16] != null
                                     ? columns[16].Substring(0, Math.Min(columns[16].Length, 512))
                                     : string.Empty,                                                                                      //品目構成固有ID
                                    HierarchyLevel = columns.Length > 18 && columns[17] != null
                                     ? columns[17].Substring(0, Math.Min(columns[17].Length, 512))
                                     : string.Empty,                                                                                      //階層レベル
                                    ItemNumber1 = columns.Length > 19 && columns[18] != null
                                     ? columns[18].Substring(0, Math.Min(columns[18].Length, 512))
                                     : string.Empty,                                                                                      //品目番号1
                                    ItemNumber2 = columns.Length > 20 && columns[19] != null
                                     ? columns[19].Substring(0, Math.Min(columns[19].Length, 512))
                                     : string.Empty,                                                                                      //品目番号2
                                    ItemNumber3 = columns.Length > 21 && columns[20] != null
                                     ? columns[20].Substring(0, Math.Min(columns[20].Length, 512))
                                     : string.Empty,                                                                                      //品目番号3
                                    ItemNumber4 = columns.Length > 22 && columns[21] != null
                                     ? columns[21].Substring(0, Math.Min(columns[21].Length, 512))
                                     : string.Empty,                                                                                      //品目番号4
                                    ItemNumber5 = columns.Length > 23 && columns[22] != null
                                     ? columns[22].Substring(0, Math.Min(columns[22].Length, 512))
                                     : string.Empty,                                                                                      //品目番号5
                                    ItemNumber6 = columns.Length > 24 && columns[23] != null
                                     ? columns[23].Substring(0, Math.Min(columns[23].Length, 512))
                                     : string.Empty,                                                                                      //品目番号6
                                    ItemNumber7 = columns.Length > 25 && columns[24] != null
                                     ? columns[24].Substring(0, Math.Min(columns[24].Length, 512))
                                     : string.Empty,                                                                                      //品目番号7
                                    ItemNumber8 = columns.Length > 26 && columns[25] != null
                                     ? columns[25].Substring(0, Math.Min(columns[25].Length, 512))
                                     : string.Empty,                                                                                      //品目番号8
                                    ItemNumber9 = columns.Length > 27 && columns[26] != null
                                     ? columns[26].Substring(0, Math.Min(columns[26].Length, 512))
                                     : string.Empty,                                                                                      //品目番号9
                                    ItemNumber10 = columns.Length > 28 && columns[27] != null
                                     ? columns[27].Substring(0, Math.Min(columns[27].Length, 512))
                                     : string.Empty,                                                                                      //品目番号10
                                    ExternalInspection = columns.Length > 29 && columns[28] != null
                                     ? columns[28].Substring(0, Math.Min(columns[28].Length, 512))
                                     : string.Empty,                                                                                      //外部検査
                                    HighPressureGasCertified = columns.Length > 30 && columns[29] != null
                                     ? columns[29].Substring(0, Math.Min(columns[29].Length, 512))
                                     : string.Empty,                                                                                      //高圧ガス認定品
                                    FourTimesPressureResistance = columns.Length > 31 && columns[30] != null
                                     ? columns[30].Substring(0, Math.Min(columns[30].Length, 512))
                                     : string.Empty,                                                                                      //4倍耐圧
                                    WoodPattern = columns.Length > 32 && columns[31] != null
                                     ? columns[31].Substring(0, Math.Min(columns[31].Length, 512))
                                     : string.Empty,                                                                                      //木型
                                    PaintProcedureExist = columns.Length > 33 && columns[32] != null
                                     ? columns[32].Substring(0, Math.Min(columns[32].Length, 512))
                                     : string.Empty,                                                                                      //塗装要領書有無
                                    PaintingNotAllowedAtGasConnection = columns.Length > 34 && columns[33] != null
                                     ? columns[33].Substring(0, Math.Min(columns[33].Length, 512))
                                     : string.Empty,                                                                                      //接ガス部塗装不可
                                    OilRestriction = columns.Length > 35 && columns[34] != null
                                     ? columns[34].Substring(0, Math.Min(columns[34].Length, 512))
                                     : string.Empty,                                                                                      //禁油処理
                                    WaterRestriction = columns.Length > 36 && columns[35] != null
                                     ? columns[35].Substring(0, Math.Min(columns[35].Length, 512))
                                     : string.Empty,                                                                                      //禁水処理
                                    ForNuclearUse = columns.Length > 37 && columns[36] != null
                                     ? columns[36].Substring(0, Math.Min(columns[36].Length, 512))
                                     : string.Empty,                                                                                      //原子力用
                                    DesignChangeNotificationNumber = columns.Length > 38 && columns[37] != null
                                     ? columns[37].Substring(0, Math.Min(columns[37].Length, 512))
                                     : string.Empty,                                                                                      //設計変更通知番号
                                    AssemblyDivision = columns.Length > 39 && columns[38] != null
                                     ? columns[38].Substring(0, Math.Min(columns[38].Length, 512))
                                     : string.Empty,                                                                                      //組立区分
                                    Remarks = columns.Length > 40 && columns[39] != null
                                     ? columns[39].Substring(0, Math.Min(columns[39].Length, 512))
                                     : string.Empty,                                                                                      //備考
                                    ManufacturerCode = columns.Length > 41 && columns[40] != null
                                     ? columns[40].Substring(0, Math.Min(columns[40].Length, 512))
                                     : string.Empty,                                                                                      //メーカーコード
                                    RequirementForStandard2_02 = columns.Length > 42 && columns[41] != null
                                     ? columns[41].Substring(0, Math.Min(columns[41].Length, 512))
                                     : string.Empty,                                                                                      //準2-02対応要否
                                    BudgetNumber = columns.Length > 43 && columns[42] != null
                                     ? columns[42].Substring(0, Math.Min(columns[42].Length, 512))
                                     : string.Empty,                                                                                      //予算書番号
                                    StrengthCalculationExist = columns.Length > 44 && columns[43] != null
                                     ? columns[43].Substring(0, Math.Min(columns[43].Length, 512))
                                     : string.Empty,                                                                                      //強度計算書有無
                                    PreDeliveryDocumentation = columns.Length > 45 && columns[44] != null
                                     ? columns[44].Substring(0, Math.Min(columns[44].Length, 512))
                                     : string.Empty,                                                                                      //調達品の事前納品図書
                                    CompletionDocumentation = columns.Length > 46 && columns[45] != null
                                     ? columns[45].Substring(0, Math.Min(columns[45].Length, 512))
                                     : string.Empty,                                                                                      //調達品の完成時納入図書
                                    ItemUniqueAttribute19 = columns.Length > 47 && columns[46] != null
                                     ? columns[46].Substring(0, Math.Min(columns[46].Length, 512))
                                     : string.Empty,                                                                                      //品目構成固有属性19
                                    ItemUniqueAttribute20 = columns.Length >= 48 && columns[47] != null
                                     ? columns[47].Substring(0, Math.Min(columns[47].Length, 512))
                                     : string.Empty,                                                                                      //品目構成固有属性20                                                                                                                                          //品目構成固有属性20
                                });
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
                    // * ファイル移送処理（E-BOM連携ファイル）
                    // *********************************************************************************
                    #region ファイル移送処理（E-BOM連携ファイル）
                    foreach (var file in filesEBOM)
                    {
                        try
                        {
                            // ファイル名を移動先フォルダのパスに設定
                            string fileName = Path.GetFileName(file);
                            string destinationPath = null;
                            if (integrationErrorType_EBOM)
                            {
                                destinationPath = Path.Combine(errorTargetFolderEBOM, fileName);
                            }
                            else
                            {
                                destinationPath = Path.Combine(successTargetFolderEBOM, fileName);
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
                            integrationErrorType_EBOM = true;
                        }
                    }
                    #endregion
                    // *********************************************************************************
                    // * データベース接続終了
                    // *********************************************************************************
                    Console.WriteLine("プログラムが終了しました。");
#if DEBUG
                    if (!integrationErrorType_EBOM)
                    {
                        Console.WriteLine("戻り値:" + 0);
                    }
                    else
                    {
                        Console.WriteLine("戻り値:" + 90);
                    }
                    Console.WriteLine("画面を閉じるにはEnterを押下してください。");
                    RTN = Console.ReadLine();
#endif
                    if (!integrationErrorType_EBOM)
                    {
                        return 0;
                    }
                    else
                    {
                        return 90;
                    }
                }
            }
            catch (OdbcException ex)
            {
                Console.WriteLine("接続エラー: " + ex.Message);
                return 97;
            }
            catch (Exception ex)
            {
                Console.WriteLine("エラーしました:" + ex.Message + " ");
                return 98;
            }
//            finally
//            {

//                Console.WriteLine("プログラムが終了しました。");
//#if DEBUG
//                Console.WriteLine("画面を閉じるにはEnterを押下してください。");
//                string RTN = Console.ReadLine();
//#endif
//            }
        }
        // Main終了
        // 以下サブモジュール／クラス

        static void GetMKUBN(OdbcConnection connection, string query, string envMode, string code, string value,
                    out string CodeDescription1, out string CodeDescription2)
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

        public class EBOMRecord
        {
            // *********************************************************************************
            // * E-BOM連携ファイルデータクラス
            // *********************************************************************************

            [CsvField(required: true, length: 10, dataType: typeof(string))] // コード番号
            public string CodeNumber { get; set; }

            [CsvField(required: true, length: 3, dataType: typeof(string))] // コード番号枝番
            public string CodeBranchNumber { get; set; }

            [CsvField(required: true, length: 25, dataType: typeof(string))] // 親品目番号
            public string ParentItemNumber { get; set; }

            [CsvField(required: false, length: 4, dataType: typeof(string))] // 風船番号(照番)
            public string BalloonNumber { get; set; }

            [CsvField(required: false, length: 6, dataType: typeof(string))] // 順序
            public string Order { get; set; }

            [CsvField(required: true, length: 25, dataType: typeof(string))] // 子品目番号
            public string ChildItemNumber { get; set; }

            [CsvField(required: false, length: 128, dataType: typeof(string))] // P *****
            public string PFlag { get; set; }

            [CsvField(required: false, length: 128, dataType: typeof(string))] // M *****
            public string MFlag { get; set; }

            [CsvField(required: true, length: 8, dataType: typeof(string))] // 員数
            public string Quantity { get; set; }

            [CsvField(required: false, length: 8, dataType: typeof(string))] // 手配数量            *****　値が入っていないケースがあるので、一旦Falseにしている
            public string OrderQuantity { get; set; }

            [CsvField(required: false, length: 25, dataType: typeof(string))] // 手配指示番号       *****　値が入っていないケースがあるので、一旦Falseにしている
            public string OrderInstructionNumber { get; set; }

            [CsvField(required: false, length: 25, dataType: typeof(string))] // まとめ手配指示番号 *****　値が入っていないケースがあるので、一旦Falseにしている
            public string SummaryOrderInstructionNumber { get; set; }

            [CsvField(required: false, length: 14, dataType: typeof(string))] // 手配依頼日         *****　値が入っていないケースがあるので、一旦Falseにしている
            public string OrderRequestDate { get; set; }

            [CsvField(required: false, length: 1, dataType: typeof(string))] // 手配品目フラグ
            public string OrderItemFlag { get; set; }

            [CsvField(required: false, length: 1, dataType: typeof(string))] // 手配完了フラグ
            public string OrderCompletionFlag { get; set; }

            [CsvField(required: false, length: 1024, dataType: typeof(string))] // 親品目構成固有ID
            public string ParentItemUniqueID { get; set; }

            [CsvField(required: true, length: 1024, dataType: typeof(string))] // 品目構成固有ID
            public string ItemUniqueID { get; set; }

            [CsvField(required: true, length: 3, dataType: typeof(string))] // 階層レベル
            public string HierarchyLevel { get; set; }

            // 品目番号1〜10
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber1 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber2 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber3 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber4 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber5 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber6 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber7 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber8 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber9 { get; set; }
            [CsvField(required: false, length: 128, dataType: typeof(string))] public string ItemNumber10 { get; set; }

            [CsvField(required: false, length: 1024 , dataType: typeof(string))] // 外部検査
            public string ExternalInspection { get; set; }

            [CsvField(required: true, length: 5, dataType: typeof(string))] // 高圧ガス認定品
            public string HighPressureGasCertified { get; set; }

            [CsvField(required: true, length: 5, dataType: typeof(string))] // 4倍耐圧
            public string FourTimesPressureResistance { get; set; }

            [CsvField(required: false, length: 1024, dataType: typeof(string))] // 木型
            public string WoodPattern { get; set; }

            [CsvField(required: true, length: 5, dataType: typeof(string))] // 塗装要領書有無
            public string PaintProcedureExist { get; set; }

            [CsvField(required: true, length: 5, dataType: typeof(string))] // 接ガス部塗装不可
            public string PaintingNotAllowedAtGasConnection { get; set; }

            [CsvField(required: false, length: 1024, dataType: typeof(string))] // 禁油処理
            public string OilRestriction { get; set; }

            [CsvField(required: false, length: 1024, dataType: typeof(string))] // 禁水処理
            public string WaterRestriction { get; set; }

            [CsvField(required: true, length: 5, dataType: typeof(string))] // 原子力用
            public string ForNuclearUse { get; set; }

            [CsvField(required: false, length: 9, dataType: typeof(string))] // 設計変更通知番号
            public string DesignChangeNotificationNumber { get; set; }

            [CsvField(required: false, length: 10, dataType: typeof(string))] // 組立区分
            public string AssemblyDivision { get; set; }

            [CsvField(required: false, length: 44, dataType: typeof(string))] // 備考
            public string Remarks { get; set; }

            [CsvField(required: false, length: 8, dataType: typeof(string))] // メーカーコード
            public string ManufacturerCode { get; set; }

            [CsvField(required: true, length: 1, dataType: typeof(string))] // 準2-02対応要否
            public string RequirementForStandard2_02 { get; set; }

            [CsvField(required: false, length: 30, dataType: typeof(string))] // 予算書番号
            public string BudgetNumber { get; set; }

            [CsvField(required: true, length: 1, dataType: typeof(string))] // 強度計算書有無
            public string StrengthCalculationExist { get; set; }

            [CsvField(required: false, length: 256, dataType: typeof(string))] // 調達品の事前納品図書
            public string PreDeliveryDocumentation { get; set; }

            [CsvField(required: false, length: 256, dataType: typeof(string))] // 調達品の完成時納入図書
            public string CompletionDocumentation { get; set; }

            [CsvField(required: false, length: 256, dataType: typeof(string))] // 品目構成固有属性19
            public string ItemUniqueAttribute19 { get; set; }

            [CsvField(required: false, length: 256, dataType: typeof(string))] // 品目構成固有属性20
            public string ItemUniqueAttribute20 { get; set; }
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
                    if (attr.DataType == typeof(Decimal) && !Decimal.TryParse(value, out _))
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

        public class ExecutionBudgetHeader
        {
            // *********************************************************************************
            // * 実行予算見出ファイルのエンティティクラス
            // *********************************************************************************
            private OdbcConnection _connection;
            private OdbcTransaction transaction;

            // コンストラクタ
            public ExecutionBudgetHeader(OdbcConnection connection, OdbcTransaction transaction)
            {
                _connection = connection;
                this.transaction = transaction;
            }

            public class ExBudgetHead{

                public int DeleteFlag { get; set; }            // 削除フラグ (0: 削除されていない, 1: 削除された)
                public string RegisteredUserCode { get; set; } // 登録担当者コード (最大5文字)
                public Decimal RegisteredDateTime { get; set; }    // 登録日時 (数値表現)
                public string RegisteredProgramID { get; set; } // 登録プログラムID (最大50文字)
                public string UpdatedUserCode { get; set; }     // 更新担当者コード (最大5文字)
                public Decimal UpdatedDateTime { get; set; }       // 更新日時 (数値表現)
                public string UpdatedProgramID { get; set; }    // 更新プログラムID (最大50文字)
                public string CodeNumber { get; set; }          // コード番号 (主キー, 最大10文字)
                public string BranchNumber { get; set; }        // 枝番 (主キー, 最大3文字)
                public Decimal Revision { get; set; }               // 実行予算リビジョン (主キー)
                public string ManufacturingDivision { get; set; } // 製造区分 (最大1文字)
                public string ProductClassificationCode { get; set; } // 製品分類コード (最大3文字)
                public Decimal OrderTotalAmount { get; set; }      // 受注合計金額 (数値表現)
                public Decimal ExecutionBudgetTotalAmount { get; set; } // 実行予算合計金額 (数値表現)
                public Decimal ForecastTotalAmount { get; set; }    // 見通し合計金額 (数値表現)
                public Decimal AchievementTotalAmount { get; set; } // 実績合計金額 (数値表現)
                public string MidScheduleUserCode { get; set; } // 中日程担当者コード (最大5文字)
                public string DesignTaskNotes { get; set; }     // 設計担当備考 (最大1024文字)
                public string AssemblyTaskNotes { get; set; }   // 組立担当備考 (最大1024文字)
                public string InspectionTaskNotes { get; set; }  // 検査担当備考 (最大1024文字)
                public string SpecialNotes { get; set; }        // 特記事項 (最大5000文字)
                public Decimal ShipmentScheduledDate { get; set; }  // 出荷予定日 (数値表現)
                public Decimal CompletionDate { get; set; }         // 完了日 (数値表現)
                public string CompletionStatus { get; set; }     // 完了状況 (最大3文字)
                public string OrderNo { get; set; }              // 受注No (最大10文字)
                public Decimal OrderLineNo { get; set; }            // 受注行No (数値表現)
                public string ExecutionBudgetApplicationStatus { get; set; } // 実行予算申請状況 (最大3文字)
                public string LatestFlag { get; set; }           // 最新フラグ (最大1文字)
                public string BRTransmissionCompletionStatus { get; set; } // BR送信完成状況 (最大3文字)
            }

            // Method to get data from the database
            public ExBudgetHead GetExBudgetHDByID(string codeNumber, string branchNumber)
            {
                ExBudgetHead exBudgetHead = null;
                string query = @"
                SELECT * FROM FHZYN 
                WHERE NOCODE = ? AND NOCDE1 = ? AND FGNEW = ? ";

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@NOCODE", codeNumber);
                    command.Parameters.AddWithValue("@NOCDE1", branchNumber);
                    command.Parameters.AddWithValue("@FGNEW", "1");
                    command.Transaction = this.transaction;

                    using (var reader = command.ExecuteReader())
                    {
                        Console.WriteLine(reader.HasRows);
                        if (reader.Read())
                        {
                            exBudgetHead = new ExBudgetHead()
                            {
                                DeleteFlag = reader.GetInt32(reader.GetOrdinal("FGDELE")),
                                RegisteredUserCode = reader.GetString(reader.GetOrdinal("SPIUSR")),
                                RegisteredDateTime = reader.GetDecimal(reader.GetOrdinal("SPIDTM")),
                                RegisteredProgramID = reader.GetString(reader.GetOrdinal("SPIPGM")),
                                UpdatedUserCode = reader.GetString(reader.GetOrdinal("SPUUSR")),
                                UpdatedDateTime = reader.GetDecimal(reader.GetOrdinal("SPUDTM")),
                                UpdatedProgramID = reader.GetString(reader.GetOrdinal("SPUPGM")),
                                CodeNumber = reader.GetString(reader.GetOrdinal("NOCODE")),
                                BranchNumber = reader.GetString(reader.GetOrdinal("NOCDE1")),
                                Revision = reader.GetDecimal(reader.GetOrdinal("NOYSRV")),
                                ManufacturingDivision = reader.GetString(reader.GetOrdinal("KBSEZO")),
                                ProductClassificationCode = reader.GetString(reader.GetOrdinal("CDSEIB")),
                                OrderTotalAmount = reader.GetDecimal(reader.GetOrdinal("ATJUGK")),
                                ExecutionBudgetTotalAmount = reader.GetDecimal(reader.GetOrdinal("ATJYGK")),
                                ForecastTotalAmount = reader.GetDecimal(reader.GetOrdinal("ATSYGK")),
                                AchievementTotalAmount = reader.GetDecimal(reader.GetOrdinal("ATJSGK")),
                                MidScheduleUserCode = reader.GetString(reader.GetOrdinal("CDCNTI")),
                                DesignTaskNotes = reader.GetString(reader.GetOrdinal("TXBKO1")),
                                AssemblyTaskNotes = reader.GetString(reader.GetOrdinal("TXBKO2")),
                                InspectionTaskNotes = reader.GetString(reader.GetOrdinal("TXBKO3")),
                                SpecialNotes = reader.GetString(reader.GetOrdinal("TXTOK1")),
                                ShipmentScheduledDate = reader.GetDecimal(reader.GetOrdinal("DTSYUK")),
                                CompletionDate = reader.GetDecimal(reader.GetOrdinal("DTKANR")),
                                CompletionStatus = reader.GetString(reader.GetOrdinal("KBSTKN")),
                                OrderNo = reader.GetString(reader.GetOrdinal("NOJUCH")),
                                OrderLineNo = reader.GetDecimal(reader.GetOrdinal("NPJUCH")),
                                ExecutionBudgetApplicationStatus = reader.GetString(reader.GetOrdinal("STJSEI")),
                                LatestFlag = reader.GetString(reader.GetOrdinal("FGNEW")),
                                BRTransmissionCompletionStatus = reader.GetString(reader.GetOrdinal("KBBRKN")),
                            };
                            return exBudgetHead;
                        }
                    }
                }
                return exBudgetHead;
            }
        }

        public class ExecutionBudgetDetail
        {
            // *********************************************************************************
            // * 実行予算見出ファイルのエンティティクラス
            // *********************************************************************************
            private OdbcConnection _connection;
            private OdbcTransaction transaction;

            // コンストラクタ
            public ExecutionBudgetDetail(OdbcConnection connection, OdbcTransaction transaction)
            {
                _connection = connection;
                this.transaction = transaction;
            }

            public class ExBudgetDetail
            {
                public int DeleteFlag { get; set; }            // 削除フラグ (0: 削除されていない, 1: 削除された)
                public string RegisteredUserCode { get; set; } // 登録担当者コード (最大5文字)
                public Decimal RegisteredDateTime { get; set; }    // 登録日時 (数値表現)
                public string RegisteredProgramID { get; set; } // 登録プログラムID (最大50文字)
                public string UpdatedUserCode { get; set; }     // 更新担当者コード (最大5文字)
                public Decimal UpdatedDateTime { get; set; }       // 更新日時 (数値表現)
                public string UpdatedProgramID { get; set; }    // 更新プログラムID (最大50文字)
                public string CodeNumber { get; set; }          // コード番号 (主キー, 最大10文字)
                public string BranchNumber { get; set; }        // 枝番 (主キー, 最大3文字)
                public Decimal Revision { get; set; }               // 実行予算リビジョン (主キー)
                public Decimal BomLineNo { get; set; }              // BOM行No (主キー)
                public Decimal DisplayLineNo { get; set; }          // 表示行No (数値表現)
                public string ArrangementInstructionNo { get; set; } // 手配指示番号 (最大25文字)
                public string SummaryArrangementInstructionNo { get; set; } // まとめ手配指示番号 (最大25文字)
                public string DesignChangeNotificationNo { get; set; } // 設計変更通知番号 (最大9文字)
                public string DesignChangeDivision { get; set; } // 設変区分 (最大1文字)
                public string AssemblyDivision { get; set; }    // 組立区分 (最大10文字)
                public Decimal HierarchyLevel { get; set; }        // 階層レベル (数値表現)
                public string ParentItemStructureUniqueID { get; set; } // 親品目構成固有ID (最大1024文字)
                public string ItemStructureUniqueID { get; set; } // 品目構成固有ID (最大1024文字)
                public string BudgetBillNo { get; set; }        // 予算書No (最大30文字)
                public Decimal BalloonNumber { get; set; }         // 風船番号 (数値表現)
                public Decimal Sequence { get; set; }              // 順序 (数値表現)
                public Decimal ParentBomLineNo { get; set; }       // 親BOM行No (数値表現)
                public string ItemCode { get; set; }            // 品目コード (最大25文字)
                public string ItemName { get; set; }            // 品目名 (最大66文字)
                public Decimal Quantity { get; set; }               // 員数 (数値表現)
                public Decimal Amount { get; set; }                 // 数量 (数値表現)
                public string ArrangementDivision { get; set; }  // 手配区分 (最大3文字)
                public string ItemTypeDivision { get; set; }     // 品目タイプ区分 (最大2文字)
                public string UnitCode { get; set; }             // 単位コード (最大3文字)
                public Decimal Dimension1 { get; set; }             // 寸法1 (数値表現)
                public Decimal Dimension2 { get; set; }             // 寸法2 (数値表現)
                public Decimal Dimension3 { get; set; }             // 寸法3 (数値表現)
                public string DrawingNo { get; set; }            // 図番/購入仕様書 (最大30文字)
                public Decimal DrawingRevision { get; set; }        // 図面リビジョン (数値表現)
                public string PaintColor { get; set; }           // 塗装色 (最大32文字)
                public string MaterialCode { get; set; }         // 材質コード (最大8文字)
                public string Specification { get; set; }        // 仕様 (最大62文字)
                public string MakerCode { get; set; }            // メーカーコード (最大8文字)
                public Decimal LT { get; set; }                      // LT (数値表現)
                public string Remarks { get; set; }               // 備考 (最大44文字)
                public string PerformanceBookHandlingDivision { get; set; } // 成績書扱い区分 (最大1文字)
                public string PurchaseSpecificationExistence { get; set; } // 購入仕様書有無 (最大1文字)
                public string MillSheetRequirement { get; set; } // ミルシート要否 (最大1文字)
                public string StrengthCalculationSheetExistence { get; set; } // 強度計算書有無 (最大1文字)
                public string ExternalInspection { get; set; }    // 外部検査 (最大1文字)
                public string CertificationRequirement { get; set; } // 検定要否 (最大1文字)
                public string DrawingRequirement { get; set; }    // 図面要否 (最大1文字)
                public string SpecificationRequirement { get; set; } // 仕様書要否 (最大1文字)
                public string HeatTreatmentRecordRequirement { get; set; } // 熱処理記録要否 (最大1文字)
                public string DimensionalConfirmationSheetRequirement { get; set; } // 寸法確認書要否 (最大1文字)
                public string HandlingInstructionSheetRequirement { get; set; } // 取扱説明書要否 (最大1文字)
                public string KHKQualificationRequirement { get; set; } // KHK合格書要否 (最大1文字)
                public string HighPressureGasCertificationRequirement { get; set; } // 高圧ガス認定書要否 (最大1文字)
                public string ProcessManagementRequirement { get; set; } // プロセス管理要否 (最大1文字)
                public string HighPressureGasCertifiedProduct { get; set; } // 高圧ガス認定品 (最大1文字)
                public string QuadruplePressureHot { get; set; } // 4倍耐圧 (最大1文字)
                public string WoodenType { get; set; }          // 木型 (最大1文字)
                public string CoatingInstructionsExistence { get; set; } // 塗装要領書有無 (最大1文字)
                public string GasConnectionPartCannotBeCoated { get; set; } // 接ガス部塗装不可 (最大1文字)
                public string OilRestriction { get; set; }       // 禁油処理 (最大3文字)
                public string WaterRestriction { get; set; }     // 禁水処理 (最大3文字)
                public string NuclearUse { get; set; }           // 原子力用 (最大1文字)
                public string PreparationForJun202 { get; set; } // 準2-02対応要否 (最大1文字)
                public string PressureTestRequirement { get; set; } // 耐圧試験要否 (最大1文字)
                public string Others { get; set; }                // その他 (最大256文字)
                public string SpecificationNo { get; set; }      // 仕様書No (最大20文字)

                // Item codes
                public string ItemCode1 { get; set; }            // 品目コード1 (最大25文字)
                public string ItemCode2 { get; set; }            // 品目コード2 (最大25文字)
                public string ItemCode3 { get; set; }            // 品目コード3 (最大25文字)
                public string ItemCode4 { get; set; }            // 品目コード4 (最大25文字)
                public string ItemCode5 { get; set; }            // 品目コード5 (最大25文字)
                public string ItemCode6 { get; set; }            // 品目コード6 (最大25文字)
                public string ItemCode7 { get; set; }            // 品目コード7 (最大25文字)
                public string ItemCode8 { get; set; }            // 品目コード8 (最大25文字)
                public string ItemCode9 { get; set; }            // 品目コード9 (最大25文字)
                public string ItemCode10 { get; set; }           // 品目コード10 (最大25文字)

                // Procurement details
                public string PreDeliveryDiagram { get; set; }   // 調達品の事前納品図 (最大256文字)
                public string CompletedDelivery { get; set; }    // 調達品の完成時納入 (最大256文字)
                public string UniqueAttribute19 { get; set; }    // 製番固有属性19 (最大256文字)
                public string UniqueAttribute20 { get; set; }    // 製番固有属性20 (最大256文字)
                public string ProcurementItemFlag { get; set; }             // 手配品目フラグ
                public string ProcurementCompletionFlag { get; set; }       // 手配完了フラグ
                public string MBOMReflectionFlag { get; set; }              // MBOM取込フラグ
                public string OriginalCodeNumber { get; set; }              // 元コード番号(S-BOM)
                public string OriginalBranchNumber { get; set; }            // 元枝番(S-BOM)
                public Decimal Revision_S_BOM { get; set; }                 // リビジョン(S-BOM)
                public string ItemUUID_S_BOM { get; set; }                  // 品目UUID(S-BOM)
            }

            //BOM行Noの最大値を取得するメソッド
            public int GetMaxBomLineNo(string codeNumber, string branchNumber, decimal exeBudgNo)
            {
                int maxBomLineNo = 0;

                var query = @"
                            SELECT COALESCE(MAX(NPBOMD), 0) FROM FBZYE WHERE NOCODE = ? AND NOCDE1 = ? AND NOYSRV = ?";

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@NOCODE", codeNumber);
                    command.Parameters.AddWithValue("@NOCDE1", branchNumber);
                    command.Parameters.AddWithValue("@NOYSRV", exeBudgNo);

                    command.Transaction = this.transaction;
                    var result = command.ExecuteScalar();
                    if (result != null)
                    {
                        maxBomLineNo = Convert.ToInt32(result);
                    }
                }
                return maxBomLineNo;
            }

            //同一コード番号、枝番、実行予算リビジョン内で、BOM行Noの最大値を取得するメソッド
            public String GetRootParentItemStructureUniqueID(string codeNumber, string branchNumber, decimal exeBudgNo)
            {
                String RootParentItemStructureUniqueID = null;

                var query = @"
                            SELECT
                                MIN(CDOYKO)   -- 親品目構成固有ID
                            FROM
                                FBZYEV99     -- 実行予算明細ファイル(E-BOM)
                            WHERE
                                  NOCODE = ? -- コード番号
                              AND NOCDE1 = ? -- 枝番
                              AND NOYSRV = ? -- 実行予算リビジョン
                              AND NOKSLV = ? -- 階層レベル
                            GROUP BY
                               NOCODE
                             , NOCDE1
                             , NOYSRV
                             , NOKSLV
                            ";

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@NOCODE", codeNumber);
                    command.Parameters.AddWithValue("@NOCDE1", branchNumber);
                    command.Parameters.AddWithValue("@NOYSRV", exeBudgNo);
                    command.Parameters.AddWithValue("@NOKSLV", 1);

                    command.Transaction = this.transaction;
                    var result = command.ExecuteScalar();
                    if (result != null)
                    {
                        RootParentItemStructureUniqueID = Convert.ToString(result);
                    }
                }
                return RootParentItemStructureUniqueID;
            }

            // Method to get data from the database
            public ExBudgetDetail GetExBudgetDTByID(string codeNumber, string branchNumber, string ItemStructureUniqueID)
            {
                ExBudgetDetail ExBudgetDtl = null;
                string query = @"
                SELECT * FROM FBZYE 
                WHERE NOCODE = ? AND NOCDE1 = ? AND CDKOKO = ?";

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@NOCODE", codeNumber);
                    command.Parameters.AddWithValue("@NOCDE1", branchNumber);
                    command.Parameters.AddWithValue("@CDKOKO", ItemStructureUniqueID);
                    command.Transaction = this.transaction;

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            ExBudgetDtl = new ExBudgetDetail()
                            {
                                DeleteFlag = reader.GetInt32(reader.GetOrdinal("FGDELE")),
                                RegisteredUserCode = reader.GetString(reader.GetOrdinal("SPIUSR")),
                                RegisteredDateTime = reader.GetDecimal(reader.GetOrdinal("SPIDTM")),
                                RegisteredProgramID = reader.GetString(reader.GetOrdinal("SPIPGM")),
                                UpdatedUserCode = reader.GetString(reader.GetOrdinal("SPUUSR")),
                                UpdatedDateTime = reader.GetDecimal(reader.GetOrdinal("SPUDTM")),
                                UpdatedProgramID = reader.GetString(reader.GetOrdinal("SPUPGM")),
                                CodeNumber = reader.GetString(reader.GetOrdinal("NOCODE")),
                                BranchNumber = reader.GetString(reader.GetOrdinal("NOCDE1")),
                                Revision = reader.GetDecimal(reader.GetOrdinal("NOYSRV")),
                                BomLineNo = reader.GetDecimal(reader.GetOrdinal("NPBOMD")),
                                DisplayLineNo = reader.GetDecimal(reader.GetOrdinal("NPDSPL")),
                                ArrangementInstructionNo = reader.GetString(reader.GetOrdinal("NOTEHI")),
                                SummaryArrangementInstructionNo = reader.GetString(reader.GetOrdinal("NOMTEH")),
                                DesignChangeNotificationNo = reader.GetString(reader.GetOrdinal("NOSKHN")),
                                DesignChangeDivision = reader.GetString(reader.GetOrdinal("KBSHEN")),
                                AssemblyDivision = reader.GetString(reader.GetOrdinal("KBKMTT")),
                                HierarchyLevel = reader.GetDecimal(reader.GetOrdinal("NOKSLV")),
                                ParentItemStructureUniqueID = reader.GetString(reader.GetOrdinal("CDOYKO")),
                                ItemStructureUniqueID = reader.GetString(reader.GetOrdinal("CDKOKO")),
                                BudgetBillNo = reader.GetString(reader.GetOrdinal("NOYOSN")),
                                BalloonNumber = reader.GetDecimal(reader.GetOrdinal("NOFSEN")),
                                Sequence = reader.GetDecimal(reader.GetOrdinal("NOSEQ")),
                                ParentBomLineNo = reader.GetDecimal(reader.GetOrdinal("NPBOMO")),
                                ItemCode = reader.GetString(reader.GetOrdinal("CDHINM")),
                                ItemName = reader.GetString(reader.GetOrdinal("NMHINM")),
                                Quantity = reader.GetDecimal(reader.GetOrdinal("QTINSU")),
                                Amount = reader.GetDecimal(reader.GetOrdinal("QTSURY")),
                                ArrangementDivision = reader.GetString(reader.GetOrdinal("KBTEHI")),
                                ItemTypeDivision = reader.GetString(reader.GetOrdinal("KBHNTY")),
                                UnitCode = reader.GetString(reader.GetOrdinal("CDTANI")),
                                Dimension1 = reader.GetDecimal(reader.GetOrdinal("VLSUN1")),
                                Dimension2 = reader.GetDecimal(reader.GetOrdinal("VLSUN2")),
                                Dimension3 = reader.GetDecimal(reader.GetOrdinal("VLSUN3")),
                                DrawingNo = reader.GetString(reader.GetOrdinal("VLZUBN")),
                                DrawingRevision = reader.GetDecimal(reader.GetOrdinal("VLZURV")),
                                PaintColor = reader.GetString(reader.GetOrdinal("VLCOLR")),
                                MaterialCode = reader.GetString(reader.GetOrdinal("CDZAIS")),
                                Specification = reader.GetString(reader.GetOrdinal("TXSHIY")),
                                MakerCode = reader.GetString(reader.GetOrdinal("CDMEKA")),
                                LT = reader.GetDecimal(reader.GetOrdinal("QTLDTM")),
                                Remarks = reader.GetString(reader.GetOrdinal("TXBIKO")),
                                PerformanceBookHandlingDivision = reader.GetString(reader.GetOrdinal("KBSESK")),
                                PurchaseSpecificationExistence = reader.GetString(reader.GetOrdinal("FGKNYS")),
                                MillSheetRequirement = reader.GetString(reader.GetOrdinal("FGMILL")),
                                StrengthCalculationSheetExistence = reader.GetString(reader.GetOrdinal("FGKYOD")),
                                ExternalInspection = reader.GetString(reader.GetOrdinal("KBGKEN")),
                                CertificationRequirement = reader.GetString(reader.GetOrdinal("FGKENT")),
                                DrawingRequirement = reader.GetString(reader.GetOrdinal("FGZUMN")),
                                SpecificationRequirement = reader.GetString(reader.GetOrdinal("FGSHIY")),
                                HeatTreatmentRecordRequirement = reader.GetString(reader.GetOrdinal("FGNETU")),
                                DimensionalConfirmationSheetRequirement = reader.GetString(reader.GetOrdinal("FGSUNK")),
                                HandlingInstructionSheetRequirement = reader.GetString(reader.GetOrdinal("FGSETU")),
                                KHKQualificationRequirement = reader.GetString(reader.GetOrdinal("FGKHKG")),
                                HighPressureGasCertificationRequirement = reader.GetString(reader.GetOrdinal("FGKGSY")),
                                ProcessManagementRequirement = reader.GetString(reader.GetOrdinal("FGPKAN")),
                                HighPressureGasCertifiedProduct = reader.GetString(reader.GetOrdinal("FGKGAS")),
                                QuadruplePressureHot = reader.GetString(reader.GetOrdinal("FG4TAI")),
                                WoodenType = reader.GetString(reader.GetOrdinal("KBKIGA")),
                                CoatingInstructionsExistence = reader.GetString(reader.GetOrdinal("FGTOSY")),
                                GasConnectionPartCannotBeCoated = reader.GetString(reader.GetOrdinal("FGSGTO")),
                                OilRestriction = reader.GetString(reader.GetOrdinal("KBKINY")),
                                WaterRestriction = reader.GetString(reader.GetOrdinal("KBKSUI")),
                                NuclearUse = reader.GetString(reader.GetOrdinal("FGGENR")),
                                PreparationForJun202 = reader.GetString(reader.GetOrdinal("FGJUN2")),
                                PressureTestRequirement = reader.GetString(reader.GetOrdinal("FGTAIA")),
                                Others = reader.GetString(reader.GetOrdinal("TXHOKA")),
                                SpecificationNo = reader.GetString(reader.GetOrdinal("TXSYSO")),
                                ItemCode1 = reader.GetString(reader.GetOrdinal("CDHI01")),
                                ItemCode2 = reader.GetString(reader.GetOrdinal("CDHI02")),
                                ItemCode3 = reader.GetString(reader.GetOrdinal("CDHI03")),
                                ItemCode4 = reader.GetString(reader.GetOrdinal("CDHI04")),
                                ItemCode5 = reader.GetString(reader.GetOrdinal("CDHI05")),
                                ItemCode6 = reader.GetString(reader.GetOrdinal("CDHI06")),
                                ItemCode7 = reader.GetString(reader.GetOrdinal("CDHI07")),
                                ItemCode8 = reader.GetString(reader.GetOrdinal("CDHI08")),
                                ItemCode9 = reader.GetString(reader.GetOrdinal("CDHI09")),
                                ItemCode10 = reader.GetString(reader.GetOrdinal("CDHI10")),
                                PreDeliveryDiagram = reader.GetString(reader.GetOrdinal("TXZK17")),
                                CompletedDelivery = reader.GetString(reader.GetOrdinal("TXZK18")),
                                UniqueAttribute19 = reader.GetString(reader.GetOrdinal("TXZK19")),
                                UniqueAttribute20 = reader.GetString(reader.GetOrdinal("TXZK20")),
                                ProcurementItemFlag = reader.GetString(reader.GetOrdinal("FGTEHI")),
                                ProcurementCompletionFlag = reader.GetString(reader.GetOrdinal("FGTKAN")),
                                MBOMReflectionFlag = reader.GetString(reader.GetOrdinal("FGMBOM")),
                                OriginalCodeNumber = reader.GetString(reader.GetOrdinal("NOMCOD")),
                                OriginalBranchNumber = reader.GetString(reader.GetOrdinal("NOMCDE")),
                                Revision_S_BOM = reader.GetDecimal(reader.GetOrdinal("NOSBRV")),
                                ItemUUID_S_BOM = reader.GetString(reader.GetOrdinal("NOHUID")),
                            };                            
                        }
                    }
                    return ExBudgetDtl;
                }
            }




            // Method to insert data into the database
            public void Insert(ExBudgetDetail ExBudgetDtl)
            {
                string query = @"
                                INSERT 
                                INTO FBZYE( 
                                      FGDELE                                    -- 削除フラグ
                                    , SPIUSR                                    -- 登録担当者コード
                                    , SPIDTM                                    -- 登録日時
                                    , SPIPGM                                    -- 登録プログラムID
                                    , SPUUSR                                    -- 更新担当者コード
                                    , SPUDTM                                    -- 更新日時
                                    , SPUPGM                                    -- 更新プログラムID
                                    , NOCODE                                    -- コード番号
                                    , NOCDE1                                    -- 枝番
                                    , NOYSRV                                    -- 実行予算リビジョン
                                    , NPBOMD                                    -- BOM行No
                                    , NPDSPL                                    -- 表示行No
                                    , NOTEHI                                    -- 手配指示番号
                                    , NOMTEH                                    -- まとめ手配指示番号
                                    , NOSKHN                                    -- 設計変更通知番号
                                    , KBSHEN                                    -- 設変区分
                                    , KBKMTT                                    -- 組立区分
                                    , NOKSLV                                    -- 階層レベル
                                    , CDOYKO                                    -- 親品目構成固有ID
                                    , CDKOKO                                    -- 品目構成固有ID
                                    , NOYOSN                                    -- 予算書No
                                    , NOFSEN                                    -- 風船番号
                                    , NOSEQ                                     -- 順序
                                    , NPBOMO                                    -- 親BOM行No
                                    , CDHINM                                    -- 品目コード
                                    , NMHINM                                    -- 品目名
                                    , QTINSU                                    -- 員数
                                    , QTSURY                                    -- 数量
                                    , KBTEHI                                    -- 手配区分
                                    , KBHNTY                                    -- 品目タイプ区分
                                    , CDTANI                                    -- 単位コード
                                    , VLSUN1                                    -- 寸法1
                                    , VLSUN2                                    -- 寸法2
                                    , VLSUN3                                    -- 寸法3
                                    , VLZUBN                                    -- 図番/購入仕様書
                                    , VLZURV                                    -- 図面リビジョン
                                    , VLCOLR                                    -- 塗装色
                                    , CDZAIS                                    -- 材質コード
                                    , TXSHIY                                    -- 仕様
                                    , CDMEKA                                    -- メーカーコード
                                    , QTLDTM                                    -- LT
                                    , TXBIKO                                    -- 備考
                                    , KBSESK                                    -- 成績書扱い区分
                                    , FGKNYS                                    -- 購入仕様書有無
                                    , FGMILL                                    -- ミルシート要否
                                    , FGKYOD                                    -- 強度計算書有無
                                    , KBGKEN                                    -- 外部検査
                                    , FGKENT                                    -- 検定要否
                                    , FGZUMN                                    -- 図面要否
                                    , FGSHIY                                    -- 仕様書要否
                                    , FGNETU                                    -- 熱処理記録要否
                                    , FGSUNK                                    -- 寸法確認書要否
                                    , FGSETU                                    -- 取扱説明書要否
                                    , FGKHKG                                    -- KHK合格書要否
                                    , FGKGSY                                    -- 高圧ガス認定書要否
                                    , FGPKAN                                    -- プロセス管理要否
                                    , FGKGAS                                    -- 高圧ガス認定品
                                    , FG4TAI                                    -- 4倍耐圧
                                    , KBKIGA                                    -- 木型
                                    , FGTOSY                                    -- 塗装要領書有無
                                    , FGSGTO                                    -- 接ガス部塗装不可
                                    , KBKINY                                    -- 禁油処理
                                    , KBKSUI                                    -- 禁水処理
                                    , FGGENR                                    -- 原子力用
                                    , FGJUN2                                    -- 準2-02対応要否
                                    , FGTAIA                                    -- 耐圧試験要否
                                    , TXHOKA                                    -- その他
                                    , TXSYSO                                    -- 仕様書No
                                    , CDHI01                                    -- 品目コード1
                                    , CDHI02                                    -- 品目コード2
                                    , CDHI03                                    -- 品目コード3
                                    , CDHI04                                    -- 品目コード4
                                    , CDHI05                                    -- 品目コード5
                                    , CDHI06                                    -- 品目コード6
                                    , CDHI07                                    -- 品目コード7
                                    , CDHI08                                    -- 品目コード8
                                    , CDHI09                                    -- 品目コード9
                                    , CDHI10                                    -- 品目コード10
                                    , TXZK17                                    -- 調達品の事前納品図
                                    , TXZK18                                    -- 調達品の完成時納入
                                    , TXZK19                                    -- 製番固有属性19
                                    , TXZK20                                    -- 製番固有属性20
                                    , FGTEHI                                    -- 手配品目フラグ
                                    , FGTKAN                                    -- 手配完了フラグ
                                    , FGMBOM                                    -- MBOM反映フラグ
                                    , NOMCOD                                    -- 元コード番号(S-BO
                                    , NOMCDE                                    -- 元枝番(S-BOM)
                                    , NOSBRV                                    -- リビジョン(S-BOM)
                                    , NOHUID                                    -- 品目UUID(S-BOM)
                                ) 
                                VALUES ( 
                                      ?                                         -- 削除フラグ
                                    , ?                                         -- 登録担当者コード
                                    , ?                                         -- 登録日時
                                    , ?                                         -- 登録プログラムID
                                    , ?                                         -- 更新担当者コード
                                    , ?                                         -- 更新日時
                                    , ?                                         -- 更新プログラムID
                                    , ?                                         -- コード番号
                                    , ?                                         -- 枝番
                                    , ?                                         -- 実行予算リビジョン
                                    , ?                                         -- BOM行No
                                    , ?                                         -- 表示行No
                                    , ?                                         -- 手配指示番号
                                    , ?                                         -- まとめ手配指示番号
                                    , ?                                         -- 設計変更通知番号
                                    , ?                                         -- 設変区分
                                    , ?                                         -- 組立区分
                                    , ?                                         -- 階層レベル
                                    , ?                                         -- 親品目構成固有ID
                                    , ?                                         -- 品目構成固有ID
                                    , ?                                         -- 予算書No
                                    , ?                                         -- 風船番号
                                    , ?                                         -- 順序
                                    , ?                                         -- 親BOM行No
                                    , ?                                         -- 品目コード
                                    , ?                                         -- 品目名
                                    , ?                                         -- 員数
                                    , ?                                         -- 数量
                                    , ?                                         -- 手配区分
                                    , ?                                         -- 品目タイプ区分
                                    , ?                                         -- 単位コード
                                    , ?                                         -- 寸法1
                                    , ?                                         -- 寸法2
                                    , ?                                         -- 寸法3
                                    , ?                                         -- 図番/購入仕様書
                                    , ?                                         -- 図面リビジョン
                                    , ?                                         -- 塗装色
                                    , ?                                         -- 材質コード
                                    , ?                                         -- 仕様
                                    , ?                                         -- メーカーコード
                                    , ?                                         -- LT
                                    , ?                                         -- 備考
                                    , ?                                         -- 成績書扱い区分
                                    , ?                                         -- 購入仕様書有無
                                    , ?                                         -- ミルシート要否
                                    , ?                                         -- 強度計算書有無
                                    , ?                                         -- 外部検査
                                    , ?                                         -- 検定要否
                                    , ?                                         -- 図面要否
                                    , ?                                         -- 仕様書要否
                                    , ?                                         -- 熱処理記録要否
                                    , ?                                         -- 寸法確認書要否
                                    , ?                                         -- 取扱説明書要否
                                    , ?                                         -- KHK合格書要否
                                    , ?                                         -- 高圧ガス認定書要否
                                    , ?                                         -- プロセス管理要否
                                    , ?                                         -- 高圧ガス認定品
                                    , ?                                         -- 4倍耐圧
                                    , ?                                         -- 木型
                                    , ?                                         -- 塗装要領書有無
                                    , ?                                         -- 接ガス部塗装不可
                                    , ?                                         -- 禁油処理
                                    , ?                                         -- 禁水処理
                                    , ?                                         -- 原子力用
                                    , ?                                         -- 準2-02対応要否
                                    , ?                                         -- 耐圧試験要否
                                    , ?                                         -- その他
                                    , ?                                         -- 仕様書No
                                    , ?                                         -- 品目コード1
                                    , ?                                         -- 品目コード2
                                    , ?                                         -- 品目コード3
                                    , ?                                         -- 品目コード4
                                    , ?                                         -- 品目コード5
                                    , ?                                         -- 品目コード6
                                    , ?                                         -- 品目コード7
                                    , ?                                         -- 品目コード8
                                    , ?                                         -- 品目コード9
                                    , ?                                         -- 品目コード10
                                    , ?                                         -- 調達品の事前納品図
                                    , ?                                         -- 調達品の完成時納入
                                    , ?                                         -- 製番固有属性19
                                    , ?                                         -- 製番固有属性20
                                    , ?                                         -- 手配品目フラグ
                                    , ?                                         -- 手配完了フラグ
                                    , ?                                         -- MBOM反映フラグ
                                    , ?                                         -- 元コード番号(S-BO
                                    , ?                                         -- 元枝番(S-BOM)
                                    , ?                                         -- リビジョン(S-BOM)
                                    , ?                                         -- 品目UUID(S-BOM)
                                )";

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", ExBudgetDtl.DeleteFlag);
                    command.Parameters.AddWithValue("@SPIUSR", ExBudgetDtl.RegisteredUserCode);
                    command.Parameters.AddWithValue("@SPIDTM", ExBudgetDtl.RegisteredDateTime);
                    command.Parameters.AddWithValue("@SPIPGM", ExBudgetDtl.RegisteredProgramID);
                    command.Parameters.AddWithValue("@SPUUSR", ExBudgetDtl.UpdatedUserCode);
                    command.Parameters.AddWithValue("@SPUDTM", ExBudgetDtl.UpdatedDateTime);
                    command.Parameters.AddWithValue("@SPUPGM", ExBudgetDtl.UpdatedProgramID);
                    command.Parameters.AddWithValue("@NOCODE", ExBudgetDtl.CodeNumber);
                    command.Parameters.AddWithValue("@NOCDE1", ExBudgetDtl.BranchNumber);
                    command.Parameters.AddWithValue("@NOYSRV", ExBudgetDtl.Revision);
                    command.Parameters.AddWithValue("@NPBOMD", ExBudgetDtl.BomLineNo);
                    command.Parameters.AddWithValue("@NPDSPL", ExBudgetDtl.DisplayLineNo);
                    command.Parameters.AddWithValue("@NOTEHI", ExBudgetDtl.ArrangementInstructionNo);
                    command.Parameters.AddWithValue("@NOMTEH", ExBudgetDtl.SummaryArrangementInstructionNo);
                    command.Parameters.AddWithValue("@NOSKHN", ExBudgetDtl.DesignChangeNotificationNo);
                    command.Parameters.AddWithValue("@KBSHEN", ExBudgetDtl.DesignChangeDivision);
                    command.Parameters.AddWithValue("@KBKMTT", ExBudgetDtl.AssemblyDivision);
                    command.Parameters.AddWithValue("@NOKSLV", ExBudgetDtl.HierarchyLevel);
                    command.Parameters.AddWithValue("@CDOYKO", ExBudgetDtl.ParentItemStructureUniqueID);
                    command.Parameters.AddWithValue("@CDKOKO", ExBudgetDtl.ItemStructureUniqueID);
                    command.Parameters.AddWithValue("@NOYOSN", ExBudgetDtl.BudgetBillNo);
                    command.Parameters.AddWithValue("@NOFSEN", ExBudgetDtl.BalloonNumber);
                    command.Parameters.AddWithValue("@NOSEQ", ExBudgetDtl.Sequence);
                    command.Parameters.AddWithValue("@NPBOMO", ExBudgetDtl.ParentBomLineNo);
                    command.Parameters.AddWithValue("@CDHINM", ExBudgetDtl.ItemCode);
                    command.Parameters.AddWithValue("@NMHINM", ExBudgetDtl.ItemName);
                    command.Parameters.AddWithValue("@QTINSU", ExBudgetDtl.Quantity);
                    command.Parameters.AddWithValue("@QTSURY", ExBudgetDtl.Amount);
                    command.Parameters.AddWithValue("@KBTEHI", ExBudgetDtl.ArrangementDivision);
                    command.Parameters.AddWithValue("@KBHNTY", ExBudgetDtl.ItemTypeDivision);
                    command.Parameters.AddWithValue("@CDTANI", ExBudgetDtl.UnitCode);
                    command.Parameters.AddWithValue("@VLSUN1", ExBudgetDtl.Dimension1);
                    command.Parameters.AddWithValue("@VLSUN2", ExBudgetDtl.Dimension2);
                    command.Parameters.AddWithValue("@VLSUN3", ExBudgetDtl.Dimension3);
                    command.Parameters.AddWithValue("@VLZUBN", ExBudgetDtl.DrawingNo);
                    command.Parameters.AddWithValue("@VLZURV", ExBudgetDtl.DrawingRevision);
                    command.Parameters.AddWithValue("@VLCOLR", ExBudgetDtl.PaintColor);
                    command.Parameters.AddWithValue("@CDZAIS", ExBudgetDtl.MaterialCode);
                    command.Parameters.AddWithValue("@TXSHIY", ExBudgetDtl.Specification);
                    command.Parameters.AddWithValue("@CDMEKA", ExBudgetDtl.MakerCode);
                    command.Parameters.AddWithValue("@QTLDTM", ExBudgetDtl.LT);
                    command.Parameters.AddWithValue("@TXBIKO", ExBudgetDtl.Remarks);
                    command.Parameters.AddWithValue("@KBSESK", ExBudgetDtl.PerformanceBookHandlingDivision);
                    command.Parameters.AddWithValue("@FGKNYS", ExBudgetDtl.PurchaseSpecificationExistence);
                    command.Parameters.AddWithValue("@FGMILL", ExBudgetDtl.MillSheetRequirement);
                    command.Parameters.AddWithValue("@FGKYOD", ExBudgetDtl.StrengthCalculationSheetExistence);
                    command.Parameters.AddWithValue("@KBGKEN", ExBudgetDtl.ExternalInspection);
                    command.Parameters.AddWithValue("@FGKENT", ExBudgetDtl.CertificationRequirement);
                    command.Parameters.AddWithValue("@FGZUMN", ExBudgetDtl.DrawingRequirement);
                    command.Parameters.AddWithValue("@FGSHIY", ExBudgetDtl.SpecificationRequirement);
                    command.Parameters.AddWithValue("@FGNETU", ExBudgetDtl.HeatTreatmentRecordRequirement);
                    command.Parameters.AddWithValue("@FGSUNK", ExBudgetDtl.DimensionalConfirmationSheetRequirement);
                    command.Parameters.AddWithValue("@FGSETU", ExBudgetDtl.HandlingInstructionSheetRequirement);
                    command.Parameters.AddWithValue("@FGKHKG", ExBudgetDtl.KHKQualificationRequirement);
                    command.Parameters.AddWithValue("@FGKGSY", ExBudgetDtl.HighPressureGasCertificationRequirement);
                    command.Parameters.AddWithValue("@FGPKAN", ExBudgetDtl.ProcessManagementRequirement);
                    command.Parameters.AddWithValue("@FGKGAS", ExBudgetDtl.HighPressureGasCertifiedProduct);
                    command.Parameters.AddWithValue("@FG4TAI", ExBudgetDtl.QuadruplePressureHot);
                    command.Parameters.AddWithValue("@KBKIGA", ExBudgetDtl.WoodenType);
                    command.Parameters.AddWithValue("@FGTOSY", ExBudgetDtl.CoatingInstructionsExistence);
                    command.Parameters.AddWithValue("@FGSGTO", ExBudgetDtl.GasConnectionPartCannotBeCoated);
                    command.Parameters.AddWithValue("@KBKINY", ExBudgetDtl.OilRestriction);
                    command.Parameters.AddWithValue("@KBKSUI", ExBudgetDtl.WaterRestriction);
                    command.Parameters.AddWithValue("@FGGENR", ExBudgetDtl.NuclearUse);
                    command.Parameters.AddWithValue("@FGJUN2", ExBudgetDtl.PreparationForJun202);
                    command.Parameters.AddWithValue("@FGTAIA", ExBudgetDtl.PressureTestRequirement);
                    command.Parameters.AddWithValue("@TXHOKA", ExBudgetDtl.Others);
                    command.Parameters.AddWithValue("@TXSYSO", ExBudgetDtl.SpecificationNo);
                    command.Parameters.AddWithValue("@CDHI01", ExBudgetDtl.ItemCode1);
                    command.Parameters.AddWithValue("@CDHI02", ExBudgetDtl.ItemCode2);
                    command.Parameters.AddWithValue("@CDHI03", ExBudgetDtl.ItemCode3);
                    command.Parameters.AddWithValue("@CDHI04", ExBudgetDtl.ItemCode4);
                    command.Parameters.AddWithValue("@CDHI05", ExBudgetDtl.ItemCode5);
                    command.Parameters.AddWithValue("@CDHI06", ExBudgetDtl.ItemCode6);
                    command.Parameters.AddWithValue("@CDHI07", ExBudgetDtl.ItemCode7);
                    command.Parameters.AddWithValue("@CDHI08", ExBudgetDtl.ItemCode8);
                    command.Parameters.AddWithValue("@CDHI09", ExBudgetDtl.ItemCode9);
                    command.Parameters.AddWithValue("@CDHI10", ExBudgetDtl.ItemCode10);
                    command.Parameters.AddWithValue("@TXZK17", ExBudgetDtl.PreDeliveryDiagram);
                    command.Parameters.AddWithValue("@TXZK18", ExBudgetDtl.CompletedDelivery);
                    command.Parameters.AddWithValue("@TXZK19", ExBudgetDtl.UniqueAttribute19);
                    command.Parameters.AddWithValue("@TXZK20", ExBudgetDtl.UniqueAttribute20);
                    command.Parameters.AddWithValue("@FGTEHI", ExBudgetDtl.ProcurementItemFlag);
                    command.Parameters.AddWithValue("@FGTKAN", ExBudgetDtl.ProcurementCompletionFlag);
                    command.Parameters.AddWithValue("@FGMBOM", ExBudgetDtl.MBOMReflectionFlag);
                    command.Parameters.AddWithValue("@NOMCOD", ExBudgetDtl.OriginalCodeNumber);
                    command.Parameters.AddWithValue("@NOMCDE", ExBudgetDtl.OriginalBranchNumber);
                    command.Parameters.AddWithValue("@NOSBRV", ExBudgetDtl.Revision_S_BOM);
                    command.Parameters.AddWithValue("@NOHUID", ExBudgetDtl.ItemUUID_S_BOM);
                    // Execute the Insert command
                    command.Transaction = this.transaction;
                    int paramIndex = 0;
                    foreach (OdbcParameter param in command.Parameters)
                    {
                        paramIndex++;
                        var val = param.Value?.ToString() ?? "";
                        var byteCount = string.IsNullOrEmpty(val) ? 0 : Encoding.GetEncoding("shift_jis").GetByteCount(val);
                        Console.WriteLine($"第{paramIndex}番目 {param.ParameterName}: {val} ({byteCount}バイト)");
                    }
                    command.ExecuteNonQuery();
                }
            }
            public void Update(ExBudgetDetail ExBudgetDtl)
            {
                string query = @"
                            UPDATE FBZYE SET 
                                SPUUSR = ?, 
                                SPUDTM = ?, 
                                SPUPGM = ?, 
                                FGTKAN = ?
                            WHERE NOCODE = ? 
                              AND NOCDE1 = ? 
                              AND NOYSRV = ? 
                              AND KBKMTT = ?";

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@SPUUSR", ExBudgetDtl.UpdatedUserCode);
                    command.Parameters.AddWithValue("@SPUDTM", ExBudgetDtl.UpdatedDateTime);
                    command.Parameters.AddWithValue("@SPUPGM", ExBudgetDtl.UpdatedProgramID);
                    command.Parameters.AddWithValue("@FGTKAN", "1");
                    command.Parameters.AddWithValue("@NOCODE", ExBudgetDtl.CodeNumber);
                    command.Parameters.AddWithValue("@NOCDE1", ExBudgetDtl.BranchNumber);
                    command.Parameters.AddWithValue("@NOYSRV", ExBudgetDtl.Revision);
                    command.Parameters.AddWithValue("@KBKMTT", ExBudgetDtl.AssemblyDivision);

                    command.Transaction = this.transaction;
                    command.ExecuteNonQuery();
                }
            }
            public void DiplaySEQUpdate(ExBudgetDetail ExBudgetDtl)
            {
                string query = @"
                            UPDATE FBZYE SET 
                                SPUUSR = ?, 
                                SPUDTM = ?, 
                                SPUPGM = ?, 
                                NPDSPL = ?
                            WHERE NOCODE = ? 
                              AND NOCDE1 = ? 
                              AND NOYSRV = ? 
                              AND CDOYKO = ?
                              AND CDKOKO = ?";

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@SPUUSR", ExBudgetDtl.UpdatedUserCode);
                    command.Parameters.AddWithValue("@SPUDTM", ExBudgetDtl.UpdatedDateTime);
                    command.Parameters.AddWithValue("@SPUPGM", ExBudgetDtl.UpdatedProgramID);
                    command.Parameters.AddWithValue("@NPDSPL", ExBudgetDtl.DisplayLineNo);
                    command.Parameters.AddWithValue("@NOCODE", ExBudgetDtl.CodeNumber);
                    command.Parameters.AddWithValue("@NOCDE1", ExBudgetDtl.BranchNumber);
                    command.Parameters.AddWithValue("@NOYSRV", ExBudgetDtl.Revision);
                    command.Parameters.AddWithValue("@CDOYKO", ExBudgetDtl.ParentItemStructureUniqueID);
                    command.Parameters.AddWithValue("@CDKOKO", ExBudgetDtl.ItemStructureUniqueID);

                    command.Transaction = this.transaction;
                    command.ExecuteNonQuery();
                }
            }
        }

        public class UniqueCodeArray
        {
            // 手配完了フラグの見直し用の対象キー退避クラス
            private HashSet<(string CodeNumber, string CodeBranchNumber, decimal Revision, string AssemblyDivision)> codes;

            public UniqueCodeArray()
            {
                codes = new HashSet<(string, string, decimal, string)>();
            }

            public void AddCode(string codeNumber, string codeBranchNumber, decimal revision, string assemblyDivision)
            {
                var codeTuple = (codeNumber, codeBranchNumber, revision, assemblyDivision);

                // 重複チェックを行い、存在しない場合にのみ追加
                if (!codes.Add(codeTuple))
                {
                    //Console.WriteLine($"重複: {codeTuple} はすでに存在します。");
                }
            }

            // コードを外部から参照できるようにするプロパティ
            public IEnumerable<(string CodeNumber, string CodeBranchNumber, Decimal Revision, string AssemblyDivision)> Codes
            {
                get { return codes; }
            }

            public void DisplayCodes()
            {
                foreach (var code in codes)
                {
                    Console.WriteLine(code);
                }
            }
        }

        public class BuildOutStructureKey
        {
            //表示順見直しの構成展開対象のキー退避クラス
            private HashSet<(string CodeNumber, string CodeBranchNumber, decimal Revision)> codes;

            public BuildOutStructureKey()
            {
                codes = new HashSet<(string, string, decimal)>();
            }

            public void AddCode(string codeNumber, string codeBranchNumber, decimal revision)
            {
                var codeTuple = (codeNumber, codeBranchNumber, revision);

                // 重複チェックを行い、存在しない場合にのみ追加
                if (!codes.Add(codeTuple))
                {
                    //Console.WriteLine($"重複: {codeTuple} はすでに存在します。");
                }
            }

            // コードを外部から参照できるようにするプロパティ
            public IEnumerable<(string CodeNumber, string CodeBranchNumber, Decimal Revision)> Codes
            {
                get { return codes; }
            }

            public void DisplayCodes()
            {
                foreach (var code in codes)
                {
                    Console.WriteLine(code);
                }
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
            public ItemMaster GetItemById(string ItemCode)
            {
                ItemMaster Item = null;

                var query = @"
                SELECT *
                FROM MHINM
                WHERE CDHINM = ?"; // SQL読み込みクエリ

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("CDHINM", ItemCode);
                    command.Transaction = this.transaction;

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            Item = new ItemMaster
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

                return Item;
            }

            // 品目マスタを挿入するメソッド
            public void InsertItem(ItemMaster Item)
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
                    command.Parameters.AddWithValue("@FGDELE", Convert.ToDecimal(Item.FGDELE));
                    command.Parameters.AddWithValue("@SPIUSR", string.IsNullOrEmpty(Item.SPIUSR) ? (object)DBNull.Value : Item.SPIUSR);
                    command.Parameters.AddWithValue("@SPIDTM", Convert.ToDecimal(Item.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPIPGM", string.IsNullOrEmpty(Item.SPIPGM) ? (object)DBNull.Value : Item.SPIPGM);
                    command.Parameters.AddWithValue("@SPUUSR", string.IsNullOrEmpty(Item.SPUUSR) ? (object)DBNull.Value : Item.SPUUSR);
                    command.Parameters.AddWithValue("@SPUDTM", Convert.ToDecimal(Item.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPUPGM", string.IsNullOrEmpty(Item.SPUPGM) ? (object)DBNull.Value : Item.SPUPGM);
                    command.Parameters.AddWithValue("@CDHINM", string.IsNullOrEmpty(Item.CDHINM) ? (object)DBNull.Value : Item.CDHINM);
                    command.Parameters.AddWithValue("@NMHINM", string.IsNullOrEmpty(Item.NMHINM) ? (object)DBNull.Value : Item.NMHINM);
                    command.Parameters.AddWithValue("@NRHINM", string.IsNullOrEmpty(Item.NRHINM) ? (object)DBNull.Value : Item.NRHINM);
                    command.Parameters.AddWithValue("@NKHINM", string.IsNullOrEmpty(Item.NKHINM) ? (object)DBNull.Value : Item.NKHINM);
                    command.Parameters.AddWithValue("@ENHINM", string.IsNullOrEmpty(Item.ENHINM) ? (object)DBNull.Value : Item.ENHINM);
                    command.Parameters.AddWithValue("@ERHINM", string.IsNullOrEmpty(Item.ERHINM) ? (object)DBNull.Value : Item.ERHINM);
                    command.Parameters.AddWithValue("@CDHINS", string.IsNullOrEmpty(Item.CDHINS) ? (object)DBNull.Value : Item.CDHINS);
                    command.Parameters.AddWithValue("@NMHISJ", string.IsNullOrEmpty(Item.NMHISJ) ? (object)DBNull.Value : Item.NMHISJ);
                    command.Parameters.AddWithValue("@NMHISE", string.IsNullOrEmpty(Item.NMHISE) ? (object)DBNull.Value : Item.NMHISE);
                    command.Parameters.AddWithValue("@CDTANI", string.IsNullOrEmpty(Item.CDTANI) ? (object)DBNull.Value : Item.CDTANI);
                    command.Parameters.AddWithValue("@KBUISI", string.IsNullOrEmpty(Item.KBUISI) ? (object)DBNull.Value : Item.KBUISI);
                    command.Parameters.AddWithValue("@KBCGHN", string.IsNullOrEmpty(Item.KBCGHN) ? (object)DBNull.Value : Item.KBCGHN);
                    command.Parameters.AddWithValue("@KBYOTO", string.IsNullOrEmpty(Item.KBYOTO) ? (object)DBNull.Value : Item.KBYOTO);
                    command.Parameters.AddWithValue("@KBKMTT", string.IsNullOrEmpty(Item.KBKMTT) ? (object)DBNull.Value : Item.KBKMTT);
                    command.Parameters.AddWithValue("@KBHNTY", string.IsNullOrEmpty(Item.KBHNTY) ? (object)DBNull.Value : Item.KBHNTY);
                    command.Parameters.AddWithValue("@CDHINB", string.IsNullOrEmpty(Item.CDHINB) ? (object)DBNull.Value : Item.CDHINB);
                    command.Parameters.AddWithValue("@CDHNGP", string.IsNullOrEmpty(Item.CDHNGP) ? (object)DBNull.Value : Item.CDHNGP);
                    command.Parameters.AddWithValue("@NOREVS", Convert.ToDecimal(Item.NOREVS)); // int への適切な変換
                    command.Parameters.AddWithValue("@KBSING", string.IsNullOrEmpty(Item.KBSING) ? (object)DBNull.Value : Item.KBSING);
                    command.Parameters.AddWithValue("@VLZUBN", string.IsNullOrEmpty(Item.VLZUBN) ? (object)DBNull.Value : Item.VLZUBN);
                    command.Parameters.AddWithValue("@CDHHIN", string.IsNullOrEmpty(Item.CDHHIN) ? (object)DBNull.Value : Item.CDHHIN);
                    command.Parameters.AddWithValue("@NMBCLS", string.IsNullOrEmpty(Item.NMBCLS) ? (object)DBNull.Value : Item.NMBCLS);
                    command.Parameters.AddWithValue("@TXZUMN", string.IsNullOrEmpty(Item.TXZUMN) ? (object)DBNull.Value : Item.TXZUMN);
                    command.Parameters.AddWithValue("@TXBRTX", string.IsNullOrEmpty(Item.TXBRTX) ? (object)DBNull.Value : Item.TXBRTX);
                    command.Parameters.AddWithValue("@KBHREN", string.IsNullOrEmpty(Item.KBHREN) ? (object)DBNull.Value : Item.KBHREN);
                    command.Parameters.AddWithValue("@NOSOZU", string.IsNullOrEmpty(Item.NOSOZU) ? (object)DBNull.Value : Item.NOSOZU);
                    command.Parameters.AddWithValue("@NOKIKN", string.IsNullOrEmpty(Item.NOKIKN) ? (object)DBNull.Value : Item.NOKIKN);
                    command.Parameters.AddWithValue("@TXZAIS", string.IsNullOrEmpty(Item.TXZAIS) ? (object)DBNull.Value : Item.TXZAIS);
                    command.Parameters.AddWithValue("@CDZAIS1", string.IsNullOrEmpty(Item.CDZAIS1) ? (object)DBNull.Value : Item.CDZAIS1);
                    command.Parameters.AddWithValue("@CDZAIS2", string.IsNullOrEmpty(Item.CDZAIS2) ? (object)DBNull.Value : Item.CDZAIS2);
                    command.Parameters.AddWithValue("@CDZAIS3", string.IsNullOrEmpty(Item.CDZAIS3) ? (object)DBNull.Value : Item.CDZAIS3);
                    command.Parameters.AddWithValue("@CDZAIR", string.IsNullOrEmpty(Item.CDZAIR) ? (object)DBNull.Value : Item.CDZAIR);
                    command.Parameters.AddWithValue("@CDGKAS", string.IsNullOrEmpty(Item.CDGKAS) ? (object)DBNull.Value : Item.CDGKAS);
                    command.Parameters.AddWithValue("@NOTANA", string.IsNullOrEmpty(Item.NOTANA) ? (object)DBNull.Value : Item.NOTANA);
                    command.Parameters.AddWithValue("@CDKISH", string.IsNullOrEmpty(Item.CDKISH) ? (object)DBNull.Value : Item.CDKISH);
                    command.Parameters.AddWithValue("@KBZAIR", string.IsNullOrEmpty(Item.KBZAIR) ? (object)DBNull.Value : Item.KBZAIR);
                    command.Parameters.AddWithValue("@KBZAIK", string.IsNullOrEmpty(Item.KBZAIK) ? (object)DBNull.Value : Item.KBZAIK);
                    command.Parameters.AddWithValue("@KBLTKR", string.IsNullOrEmpty(Item.KBLTKR) ? (object)DBNull.Value : Item.KBLTKR);
                    command.Parameters.AddWithValue("@KBSESK", string.IsNullOrEmpty(Item.KBSESK) ? (object)DBNull.Value : Item.KBSESK);
                    command.Parameters.AddWithValue("@FGMILL", string.IsNullOrEmpty(Item.FGMILL) ? (object)DBNull.Value : Item.FGMILL);
                    command.Parameters.AddWithValue("@FGKYOD", string.IsNullOrEmpty(Item.FGKYOD) ? (object)DBNull.Value : Item.FGKYOD);
                    command.Parameters.AddWithValue("@FGKENT", string.IsNullOrEmpty(Item.FGKENT) ? (object)DBNull.Value : Item.FGKENT);
                    command.Parameters.AddWithValue("@FGSIJI", string.IsNullOrEmpty(Item.FGSIJI) ? (object)DBNull.Value : Item.FGSIJI);
                    command.Parameters.AddWithValue("@FGZUMN", string.IsNullOrEmpty(Item.FGZUMN) ? (object)DBNull.Value : Item.FGZUMN);
                    command.Parameters.AddWithValue("@FGSHIY", string.IsNullOrEmpty(Item.FGSHIY) ? (object)DBNull.Value : Item.FGSHIY);
                    command.Parameters.AddWithValue("@FGNETU", string.IsNullOrEmpty(Item.FGNETU) ? (object)DBNull.Value : Item.FGNETU);
                    command.Parameters.AddWithValue("@FGSUNK", string.IsNullOrEmpty(Item.FGSUNK) ? (object)DBNull.Value : Item.FGSUNK);
                    command.Parameters.AddWithValue("@FGSETU", string.IsNullOrEmpty(Item.FGSETU) ? (object)DBNull.Value : Item.FGSETU);
                    command.Parameters.AddWithValue("@FGKHKG", string.IsNullOrEmpty(Item.FGKHKG) ? (object)DBNull.Value : Item.FGKHKG);
                    command.Parameters.AddWithValue("@FGKGSY", string.IsNullOrEmpty(Item.FGKGSY) ? (object)DBNull.Value : Item.FGKGSY);
                    command.Parameters.AddWithValue("@FGPKAN", string.IsNullOrEmpty(Item.FGPKAN) ? (object)DBNull.Value : Item.FGPKAN);
                    command.Parameters.AddWithValue("@FGZKEN", string.IsNullOrEmpty(Item.FGZKEN) ? (object)DBNull.Value : Item.FGZKEN);
                    command.Parameters.AddWithValue("@FGTANS", string.IsNullOrEmpty(Item.FGTANS) ? (object)DBNull.Value : Item.FGTANS);
                    command.Parameters.AddWithValue("@FGZTOS", string.IsNullOrEmpty(Item.FGZTOS) ? (object)DBNull.Value : Item.FGZTOS);
                    command.Parameters.AddWithValue("@FGZASK", string.IsNullOrEmpty(Item.FGZASK) ? (object)DBNull.Value : Item.FGZASK);
                    command.Parameters.AddWithValue("@FGTJIG", string.IsNullOrEmpty(Item.FGTJIG) ? (object)DBNull.Value : Item.FGTJIG);
                    command.Parameters.AddWithValue("@KBCYLI", string.IsNullOrEmpty(Item.KBCYLI) ? (object)DBNull.Value : Item.KBCYLI);
                    command.Parameters.AddWithValue("@KBGKAK", string.IsNullOrEmpty(Item.KBGKAK) ? (object)DBNull.Value : Item.KBGKAK);
                    command.Parameters.AddWithValue("@KBTEHI", string.IsNullOrEmpty(Item.KBTEHI) ? (object)DBNull.Value : Item.KBTEHI);
                    command.Parameters.AddWithValue("@KBNYTS", string.IsNullOrEmpty(Item.KBNYTS) ? (object)DBNull.Value : Item.KBNYTS);
                    command.Parameters.AddWithValue("@KBNYKN", string.IsNullOrEmpty(Item.KBNYKN) ? (object)DBNull.Value : Item.KBNYKN);
                    command.Parameters.AddWithValue("@KBKNSA", string.IsNullOrEmpty(Item.KBKNSA) ? (object)DBNull.Value : Item.KBKNSA);
                    command.Parameters.AddWithValue("@QTLOTS", Convert.ToDecimal(Item.QTLOTS));
                    command.Parameters.AddWithValue("@QTHCSU", Convert.ToDecimal(Item.QTHCSU));
                    command.Parameters.AddWithValue("@QTIMOJ", Convert.ToDecimal(Item.QTIMOJ)); // デフォルト値
                    command.Parameters.AddWithValue("@PRIMOT", Convert.ToDecimal(Item.PRIMOT)); // デフォルト値
                    command.Parameters.AddWithValue("@VLBSUN", string.IsNullOrEmpty(Item.VLBSUN) ? (object)DBNull.Value : Item.VLBSUN);
                    command.Parameters.AddWithValue("@TXBSUN", string.IsNullOrEmpty(Item.TXBSUN) ? (object)DBNull.Value : Item.TXBSUN);
                    command.Parameters.AddWithValue("@VLSUN1", Convert.ToDecimal(Item.VLSUN1)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN2", Convert.ToDecimal(Item.VLSUN2)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN3", Convert.ToDecimal(Item.VLSUN3)); // デフォルト値
                    command.Parameters.AddWithValue("@QTLDTM", Convert.ToDecimal(Item.QTLDTM));
                    command.Parameters.AddWithValue("@TXLTTM", string.IsNullOrEmpty(Item.TXLTTM) ? (object)DBNull.Value : Item.TXLTTM);
                    command.Parameters.AddWithValue("@QTBUHJ", Convert.ToDecimal(Item.QTBUHJ)); // デフォルト値
                    command.Parameters.AddWithValue("@TXBUHJ", string.IsNullOrEmpty(Item.TXBUHJ) ? (object)DBNull.Value : Item.TXBUHJ);
                    command.Parameters.AddWithValue("@QTTNJU", Convert.ToDecimal(Item.QTTNJU)); // デフォルト値
                    command.Parameters.AddWithValue("@CDDFSH", string.IsNullOrEmpty(Item.CDDFSH) ? (object)DBNull.Value : Item.CDDFSH);
                    command.Parameters.AddWithValue("@CDMEKA", string.IsNullOrEmpty(Item.CDMEKA) ? (object)DBNull.Value : Item.CDMEKA);
                    command.Parameters.AddWithValue("@CDDAIR", string.IsNullOrEmpty(Item.CDDAIR) ? (object)DBNull.Value : Item.CDDAIR);
                    command.Parameters.AddWithValue("@TXMKAT", string.IsNullOrEmpty(Item.TXMKAT) ? (object)DBNull.Value : Item.TXMKAT);
                    command.Parameters.AddWithValue("@TXMEKA", string.IsNullOrEmpty(Item.TXMEKA) ? (object)DBNull.Value : Item.TXMEKA);
                    command.Parameters.AddWithValue("@TXBUHN", string.IsNullOrEmpty(Item.TXBUHN) ? (object)DBNull.Value : Item.TXBUHN);
                    command.Parameters.AddWithValue("@TXB512", string.IsNullOrEmpty(Item.TXB512) ? (object)DBNull.Value : Item.TXB512);
                    command.Parameters.AddWithValue("@CDSOKO", string.IsNullOrEmpty(Item.CDSOKO) ? (object)DBNull.Value : Item.CDSOKO);
                    command.Parameters.AddWithValue("@KBHNKZ", string.IsNullOrEmpty(Item.KBHNKZ) ? (object)DBNull.Value : Item.KBHNKZ);
                    command.Parameters.AddWithValue("@KBKEIG", string.IsNullOrEmpty(Item.KBKEIG) ? (object)DBNull.Value : Item.KBKEIG);
                    command.Parameters.AddWithValue("@QTHCTN", Convert.ToDecimal(Item.QTHCTN)); // デフォルト値
                    command.Parameters.AddWithValue("@QTTZAI", Convert.ToDecimal(Item.QTTZAI)); // デフォルト値
                    command.Parameters.AddWithValue("@YMSIKO", Convert.ToDecimal(Item.YMSIKO));
                    command.Parameters.AddWithValue("@QTHNEN", Convert.ToDecimal(Item.QTHNEN)); // デフォルト値
                    command.Parameters.AddWithValue("@RTKEPN", Convert.ToDecimal(Item.RTKEPN)); // デフォルト値
                    command.Parameters.AddWithValue("@CDSRIS", string.IsNullOrEmpty(Item.CDSRIS) ? (object)DBNull.Value : Item.CDSRIS);
                    command.Parameters.AddWithValue("@VLSIZE", Convert.ToDecimal(Item.VLSIZE));
                    command.Parameters.AddWithValue("@CDDCKA", string.IsNullOrEmpty(Item.CDDCKA) ? (object)DBNull.Value : Item.CDDCKA);
                    command.Parameters.AddWithValue("@CDDDKA", string.IsNullOrEmpty(Item.CDDDKA) ? (object)DBNull.Value : Item.CDDDKA);
                    command.Parameters.AddWithValue("@CDICKA", string.IsNullOrEmpty(Item.CDICKA) ? (object)DBNull.Value : Item.CDICKA);
                    command.Parameters.AddWithValue("@CDIDKA", string.IsNullOrEmpty(Item.CDIDKA) ? (object)DBNull.Value : Item.CDIDKA);
                    command.Parameters.AddWithValue("@CDSCKA", string.IsNullOrEmpty(Item.CDSCKA) ? (object)DBNull.Value : Item.CDSCKA);
                    command.Parameters.AddWithValue("@CDSDKA", string.IsNullOrEmpty(Item.CDSDKA) ? (object)DBNull.Value : Item.CDSDKA);
                    command.Parameters.AddWithValue("@PRSANK", Convert.ToDecimal(Item.PRSANK)); // デフォルト値
                    command.Parameters.AddWithValue("@CDKASH", string.IsNullOrEmpty(Item.CDKASH) ? (object)DBNull.Value : Item.CDKASH);
                    command.Parameters.AddWithValue("@ATKKOS", Convert.ToDecimal(Item.ATKKOS)); // デフォルト値

                    command.Parameters.AddWithValue("@CDHINM", Item.CDHINM); // 条件用の品目コードを追加

                    command.Transaction = this.transaction;
                    command.ExecuteNonQuery(); // 挿入を実行
                }
            }
            // 品目マスタを更新するメソッド
            public void UpdateItem(ItemMaster Item)
            {
                var query = @"
	            UPDATE MHINM
	            SET 
                    FGDELE = ?, SPIUSR = ?, SPIDTM = ?, SPIPGM = ?,
                    SPUUSR = ?, SPUDTM = ?, SPUPGM = ?,	
                    NMHINM = ?, NRHINM = ?, NKHINM = ?, ENHINM = ?, ERHINM = ?, 
	                CDHINS = ?, NMHISJ = ?, NMHISE = ?, CDTANI = ?, KBUISI = ?,
	                KBCGHN = ?, KBYOTO = ?, KBKMTT = ?, KBHNTY = ?, CDHINB = ?,
	                CDHNGP = ?, NOREVS = ?, KBSING = ?, VLZUBN = ?, CDHHIN = ?,
	                NMBCLS = ?, TXZUMN = ?, TXBRTX = ?, KBHREN = ?, NOSOZU = ?,
	                NOKIKN = ?, TXZAIS = ?, CDZAIS1 = ?, CDZAIS2 = ?, CDZAIS3 = ?,
	                CDZAIR = ?, CDGKAS = ?, NOTANA = ?, CDKISH = ?, KBZAIR = ?,
	                KBZAIK = ?, KBLTKR = ?, KBSESK = ?, FGMILL = ?, FGKYOD = ?,
	                FGKENT = ?, FGSIJI = ?, FGZUMN = ?, FGSHIY = ?, FGNETU = ?,
	                FGSUNK = ?, FGSETU = ?, FGKHKG = ?, FGKGSY = ?, FGPKAN = ?,
	                FGZKEN = ?, FGTANS = ?, FGZTOS = ?, FGZASK = ?, FGTJIG = ?,
	                KBCYLI = ?, KBGKAK = ?, KBTEHI = ?, KBNYTS = ?, KBNYKN = ?,
	                KBKNSA = ?, QTLOTS = ?, QTHCSU = ?, QTIMOJ = ?, PRIMOT = ?,
	                VLBSUN = ?, TXBSUN = ?, VLSUN1 = ?, VLSUN2 = ?, VLSUN3 = ?,
	                QTLDTM = ?, TXLTTM = ?, QTBUHJ = ?, TXBUHJ = ?, QTTNJU = ?,
	                CDDFSH = ?, CDMEKA = ?, CDDAIR = ?, TXMKAT = ?, TXMEKA = ?,
	                TXBUHN = ?, TXB512 = ?, CDSOKO = ?, KBHNKZ = ?, KBKEIG = ?,
	                QTHCTN = ?, QTTZAI = ?, YMSIKO = ?, QTHNEN = ?, RTKEPN = ?,
	                CDSRIS = ?, VLSIZE = ?, CDDCKA = ?, CDDDKA = ?, CDICKA = ?,
	                CDIDKA = ?, CDSCKA = ?, CDSDKA = ?, PRSANK = ?, CDKASH = ?,
	                ATKKOS = ?
	            WHERE CDHINM = ?"; // 更新クエリ

                using (var command = new OdbcCommand(query, _connection))
                {
                    command.Parameters.AddWithValue("@FGDELE", Convert.ToDecimal(Item.SPIDTM));
                    command.Parameters.AddWithValue("@SPIUSR", string.IsNullOrEmpty(Item.CDHINM) ? (object)DBNull.Value : Item.CDHINM);
                    command.Parameters.AddWithValue("@SPIDTM", Convert.ToDecimal(Item.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPIPGM", string.IsNullOrEmpty(Item.CDHINM) ? (object)DBNull.Value : Item.CDHINM);
                    command.Parameters.AddWithValue("@SPUUSR", string.IsNullOrEmpty(Item.CDHINM) ? (object)DBNull.Value : Item.CDHINM);
                    command.Parameters.AddWithValue("@SPUDTM", Convert.ToDecimal(Item.SPIDTM)); // int への適切な変換
                    command.Parameters.AddWithValue("@SPUPGM", string.IsNullOrEmpty(Item.CDHINM) ? (object)DBNull.Value : Item.CDHINM);
                    command.Parameters.AddWithValue("@NMHINM", string.IsNullOrEmpty(Item.NMHINM) ? (object)DBNull.Value : Item.NMHINM);
                    command.Parameters.AddWithValue("@NRHINM", string.IsNullOrEmpty(Item.NRHINM) ? (object)DBNull.Value : Item.NRHINM);
                    command.Parameters.AddWithValue("@NKHINM", string.IsNullOrEmpty(Item.NKHINM) ? (object)DBNull.Value : Item.NKHINM);
                    command.Parameters.AddWithValue("@ENHINM", string.IsNullOrEmpty(Item.ENHINM) ? (object)DBNull.Value : Item.ENHINM);
                    command.Parameters.AddWithValue("@ERHINM", string.IsNullOrEmpty(Item.ERHINM) ? (object)DBNull.Value : Item.ERHINM);
                    command.Parameters.AddWithValue("@CDHINS", string.IsNullOrEmpty(Item.CDHINS) ? (object)DBNull.Value : Item.CDHINS);
                    command.Parameters.AddWithValue("@NMHISJ", string.IsNullOrEmpty(Item.NMHISJ) ? (object)DBNull.Value : Item.NMHISJ);
                    command.Parameters.AddWithValue("@NMHISE", string.IsNullOrEmpty(Item.NMHISE) ? (object)DBNull.Value : Item.NMHISE);
                    command.Parameters.AddWithValue("@CDTANI", string.IsNullOrEmpty(Item.CDTANI) ? (object)DBNull.Value : Item.CDTANI);
                    command.Parameters.AddWithValue("@KBUISI", string.IsNullOrEmpty(Item.KBUISI) ? (object)DBNull.Value : Item.KBUISI);
                    command.Parameters.AddWithValue("@KBCGHN", string.IsNullOrEmpty(Item.KBCGHN) ? (object)DBNull.Value : Item.KBCGHN);
                    command.Parameters.AddWithValue("@KBYOTO", string.IsNullOrEmpty(Item.KBYOTO) ? (object)DBNull.Value : Item.KBYOTO);
                    command.Parameters.AddWithValue("@KBKMTT", string.IsNullOrEmpty(Item.KBKMTT) ? (object)DBNull.Value : Item.KBKMTT);
                    command.Parameters.AddWithValue("@KBHNTY", string.IsNullOrEmpty(Item.KBHNTY) ? (object)DBNull.Value : Item.KBHNTY);
                    command.Parameters.AddWithValue("@CDHINB", string.IsNullOrEmpty(Item.CDHINB) ? (object)DBNull.Value : Item.CDHINB);
                    command.Parameters.AddWithValue("@CDHNGP", string.IsNullOrEmpty(Item.CDHNGP) ? (object)DBNull.Value : Item.CDHNGP);
                    command.Parameters.AddWithValue("@NOREVS", Convert.ToDecimal(Item.NOREVS));
                    command.Parameters.AddWithValue("@KBSING", string.IsNullOrEmpty(Item.KBSING) ? (object)DBNull.Value : Item.KBSING);
                    command.Parameters.AddWithValue("@VLZUBN", string.IsNullOrEmpty(Item.VLZUBN) ? (object)DBNull.Value : Item.VLZUBN);
                    command.Parameters.AddWithValue("@CDHHIN", string.IsNullOrEmpty(Item.CDHHIN) ? (object)DBNull.Value : Item.CDHHIN);
                    command.Parameters.AddWithValue("@NMBCLS", string.IsNullOrEmpty(Item.NMBCLS) ? (object)DBNull.Value : Item.NMBCLS);
                    command.Parameters.AddWithValue("@TXZUMN", string.IsNullOrEmpty(Item.TXZUMN) ? (object)DBNull.Value : Item.TXZUMN);
                    command.Parameters.AddWithValue("@TXBRTX", string.IsNullOrEmpty(Item.TXBRTX) ? (object)DBNull.Value : Item.TXBRTX);
                    command.Parameters.AddWithValue("@KBHREN", string.IsNullOrEmpty(Item.KBHREN) ? (object)DBNull.Value : Item.KBHREN);
                    command.Parameters.AddWithValue("@NOSOZU", string.IsNullOrEmpty(Item.NOSOZU) ? (object)DBNull.Value : Item.NOSOZU);
                    command.Parameters.AddWithValue("@NOKIKN", string.IsNullOrEmpty(Item.NOKIKN) ? (object)DBNull.Value : Item.NOKIKN);
                    command.Parameters.AddWithValue("@TXZAIS", string.IsNullOrEmpty(Item.TXZAIS) ? (object)DBNull.Value : Item.TXZAIS);
                    command.Parameters.AddWithValue("@CDZAIS1", string.IsNullOrEmpty(Item.CDZAIS1) ? (object)DBNull.Value : Item.CDZAIS1);
                    command.Parameters.AddWithValue("@CDZAIS2", string.IsNullOrEmpty(Item.CDZAIS2) ? (object)DBNull.Value : Item.CDZAIS2);
                    command.Parameters.AddWithValue("@CDZAIS3", string.IsNullOrEmpty(Item.CDZAIS3) ? (object)DBNull.Value : Item.CDZAIS3);
                    command.Parameters.AddWithValue("@CDZAIR", string.IsNullOrEmpty(Item.CDZAIR) ? (object)DBNull.Value : Item.CDZAIR);
                    command.Parameters.AddWithValue("@CDGKAS", string.IsNullOrEmpty(Item.CDGKAS) ? (object)DBNull.Value : Item.CDGKAS);
                    command.Parameters.AddWithValue("@NOTANA", string.IsNullOrEmpty(Item.NOTANA) ? (object)DBNull.Value : Item.NOTANA);
                    command.Parameters.AddWithValue("@CDKISH", string.IsNullOrEmpty(Item.CDKISH) ? (object)DBNull.Value : Item.CDKISH);
                    command.Parameters.AddWithValue("@KBZAIR", string.IsNullOrEmpty(Item.KBZAIR) ? (object)DBNull.Value : Item.KBZAIR);
                    command.Parameters.AddWithValue("@KBZAIK", string.IsNullOrEmpty(Item.KBZAIK) ? (object)DBNull.Value : Item.KBZAIK);
                    command.Parameters.AddWithValue("@KBLTKR", string.IsNullOrEmpty(Item.KBLTKR) ? (object)DBNull.Value : Item.KBLTKR);
                    command.Parameters.AddWithValue("@KBSESK", string.IsNullOrEmpty(Item.KBSESK) ? (object)DBNull.Value : Item.KBSESK);
                    command.Parameters.AddWithValue("@FGMILL", string.IsNullOrEmpty(Item.FGMILL) ? (object)DBNull.Value : Item.FGMILL);
                    command.Parameters.AddWithValue("@FGKYOD", string.IsNullOrEmpty(Item.FGKYOD) ? (object)DBNull.Value : Item.FGKYOD);
                    command.Parameters.AddWithValue("@FGKENT", string.IsNullOrEmpty(Item.FGKENT) ? (object)DBNull.Value : Item.FGKENT);
                    command.Parameters.AddWithValue("@FGSIJI", string.IsNullOrEmpty(Item.FGSIJI) ? (object)DBNull.Value : Item.FGSIJI);
                    command.Parameters.AddWithValue("@FGZUMN", string.IsNullOrEmpty(Item.FGZUMN) ? (object)DBNull.Value : Item.FGZUMN);
                    command.Parameters.AddWithValue("@FGSHIY", string.IsNullOrEmpty(Item.FGSHIY) ? (object)DBNull.Value : Item.FGSHIY);
                    command.Parameters.AddWithValue("@FGNETU", string.IsNullOrEmpty(Item.FGNETU) ? (object)DBNull.Value : Item.FGNETU);
                    command.Parameters.AddWithValue("@FGSUNK", string.IsNullOrEmpty(Item.FGSUNK) ? (object)DBNull.Value : Item.FGSUNK);
                    command.Parameters.AddWithValue("@FGSETU", string.IsNullOrEmpty(Item.FGSETU) ? (object)DBNull.Value : Item.FGSETU);
                    command.Parameters.AddWithValue("@FGKHKG", string.IsNullOrEmpty(Item.FGKHKG) ? (object)DBNull.Value : Item.FGKHKG);
                    command.Parameters.AddWithValue("@FGKGSY", string.IsNullOrEmpty(Item.FGKGSY) ? (object)DBNull.Value : Item.FGKGSY);
                    command.Parameters.AddWithValue("@FGPKAN", string.IsNullOrEmpty(Item.FGPKAN) ? (object)DBNull.Value : Item.FGPKAN);
                    command.Parameters.AddWithValue("@FGZKEN", string.IsNullOrEmpty(Item.FGZKEN) ? (object)DBNull.Value : Item.FGZKEN);
                    command.Parameters.AddWithValue("@FGTANS", string.IsNullOrEmpty(Item.FGTANS) ? (object)DBNull.Value : Item.FGTANS);
                    command.Parameters.AddWithValue("@FGZTOS", string.IsNullOrEmpty(Item.FGZTOS) ? (object)DBNull.Value : Item.FGZTOS);
                    command.Parameters.AddWithValue("@FGZASK", string.IsNullOrEmpty(Item.FGZASK) ? (object)DBNull.Value : Item.FGZASK);
                    command.Parameters.AddWithValue("@FGTJIG", string.IsNullOrEmpty(Item.FGTJIG) ? (object)DBNull.Value : Item.FGTJIG);
                    command.Parameters.AddWithValue("@KBCYLI", string.IsNullOrEmpty(Item.KBCYLI) ? (object)DBNull.Value : Item.KBCYLI);
                    command.Parameters.AddWithValue("@KBGKAK", string.IsNullOrEmpty(Item.KBGKAK) ? (object)DBNull.Value : Item.KBGKAK);
                    command.Parameters.AddWithValue("@KBTEHI", Item.KBTEHI ?? "999"); // デフォルトの値
                    command.Parameters.AddWithValue("@KBNYTS", string.IsNullOrEmpty(Item.KBNYTS) ? (object)DBNull.Value : Item.KBNYTS);
                    command.Parameters.AddWithValue("@KBNYKN", string.IsNullOrEmpty(Item.KBNYKN) ? (object)DBNull.Value : Item.KBNYKN);
                    command.Parameters.AddWithValue("@KBKNSA", string.IsNullOrEmpty(Item.KBKNSA) ? (object)DBNull.Value : Item.KBKNSA);
                    command.Parameters.AddWithValue("@QTLOTS", Convert.ToDecimal(Item.QTLOTS)); // デフォルト値
                    command.Parameters.AddWithValue("@QTHCSU", Convert.ToDecimal(Item.QTHCSU)); // デフォルト値
                    command.Parameters.AddWithValue("@QTIMOJ", Convert.ToDecimal(Item.QTIMOJ)); // デフォルト値
                    command.Parameters.AddWithValue("@PRIMOT", Convert.ToDecimal(Item.PRIMOT)); // デフォルト値
                    command.Parameters.AddWithValue("@VLBSUN", string.IsNullOrEmpty(Item.VLBSUN) ? (object)DBNull.Value : Item.VLBSUN);
                    command.Parameters.AddWithValue("@TXBSUN", string.IsNullOrEmpty(Item.TXBSUN) ? (object)DBNull.Value : Item.TXBSUN);
                    command.Parameters.AddWithValue("@VLSUN1", Convert.ToDecimal(Item.VLSUN1)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN2", Convert.ToDecimal(Item.VLSUN2)); // デフォルト値
                    command.Parameters.AddWithValue("@VLSUN3", Convert.ToDecimal(Item.VLSUN3)); // デフォルト値
                    command.Parameters.AddWithValue("@QTLDTM", Convert.ToDecimal(Item.QTLDTM));
                    command.Parameters.AddWithValue("@TXLTTM", string.IsNullOrEmpty(Item.TXLTTM) ? (object)DBNull.Value : Item.TXLTTM);
                    command.Parameters.AddWithValue("@QTBUHJ", Convert.ToDecimal(Item.QTBUHJ)); // デフォルト値
                    command.Parameters.AddWithValue("@TXBUHJ", string.IsNullOrEmpty(Item.TXBUHJ) ? (object)DBNull.Value : Item.TXBUHJ);
                    command.Parameters.AddWithValue("@QTTNJU", Convert.ToDecimal(Item.QTTNJU)); // デフォルト値
                    command.Parameters.AddWithValue("@CDDFSH", string.IsNullOrEmpty(Item.CDDFSH) ? (object)DBNull.Value : Item.CDDFSH);
                    command.Parameters.AddWithValue("@CDMEKA", string.IsNullOrEmpty(Item.CDMEKA) ? (object)DBNull.Value : Item.CDMEKA);
                    command.Parameters.AddWithValue("@CDDAIR", string.IsNullOrEmpty(Item.CDDAIR) ? (object)DBNull.Value : Item.CDDAIR);
                    command.Parameters.AddWithValue("@TXMKAT", string.IsNullOrEmpty(Item.TXMKAT) ? (object)DBNull.Value : Item.TXMKAT);
                    command.Parameters.AddWithValue("@TXMEKA", string.IsNullOrEmpty(Item.TXMEKA) ? (object)DBNull.Value : Item.TXMEKA);
                    command.Parameters.AddWithValue("@TXBUHN", string.IsNullOrEmpty(Item.TXBUHN) ? (object)DBNull.Value : Item.TXBUHN);
                    command.Parameters.AddWithValue("@TXB512", string.IsNullOrEmpty(Item.TXB512) ? (object)DBNull.Value : Item.TXB512);
                    command.Parameters.AddWithValue("@CDSOKO", string.IsNullOrEmpty(Item.CDSOKO) ? (object)DBNull.Value : Item.CDSOKO);
                    command.Parameters.AddWithValue("@KBHNKZ", string.IsNullOrEmpty(Item.KBHNKZ) ? (object)DBNull.Value : Item.KBHNKZ);
                    command.Parameters.AddWithValue("@KBKEIG", string.IsNullOrEmpty(Item.KBKEIG) ? (object)DBNull.Value : Item.KBKEIG);
                    command.Parameters.AddWithValue("@QTHCTN", Convert.ToDecimal(Item.QTHCTN)); // デフォルト値
                    command.Parameters.AddWithValue("@QTTZAI", Convert.ToDecimal(Item.QTTZAI)); // デフォルト値
                    command.Parameters.AddWithValue("@YMSIKO", Convert.ToDecimal(Item.YMSIKO));
                    command.Parameters.AddWithValue("@QTHNEN", Convert.ToDecimal(Item.QTHNEN)); // デフォルト値
                    command.Parameters.AddWithValue("@RTKEPN", Convert.ToDecimal(Item.RTKEPN)); // デフォルト値
                    command.Parameters.AddWithValue("@CDSRIS", string.IsNullOrEmpty(Item.CDSRIS) ? (object)DBNull.Value : Item.CDSRIS);
                    command.Parameters.AddWithValue("@VLSIZE", Convert.ToDecimal(Item.VLSIZE));
                    command.Parameters.AddWithValue("@CDDCKA", string.IsNullOrEmpty(Item.CDDCKA) ? (object)DBNull.Value : Item.CDDCKA);
                    command.Parameters.AddWithValue("@CDDDKA", string.IsNullOrEmpty(Item.CDDDKA) ? (object)DBNull.Value : Item.CDDDKA);
                    command.Parameters.AddWithValue("@CDICKA", string.IsNullOrEmpty(Item.CDICKA) ? (object)DBNull.Value : Item.CDICKA);
                    command.Parameters.AddWithValue("@CDIDKA", string.IsNullOrEmpty(Item.CDIDKA) ? (object)DBNull.Value : Item.CDIDKA);
                    command.Parameters.AddWithValue("@CDSCKA", string.IsNullOrEmpty(Item.CDSCKA) ? (object)DBNull.Value : Item.CDSCKA);
                    command.Parameters.AddWithValue("@CDSDKA", string.IsNullOrEmpty(Item.CDSDKA) ? (object)DBNull.Value : Item.CDSDKA);
                    command.Parameters.AddWithValue("@PRSANK", Convert.ToDecimal(Item.PRSANK)); // デフォルト値
                    command.Parameters.AddWithValue("@CDKASH", string.IsNullOrEmpty(Item.CDKASH) ? (object)DBNull.Value : Item.CDKASH);
                    command.Parameters.AddWithValue("@ATKKOS", Convert.ToDecimal(Item.ATKKOS)); // デフォルト値

                    command.Parameters.AddWithValue("@CDHINM", Item.CDHINM); // 条件用の品目コードを追加
                    command.Transaction = this.transaction;
                    command.ExecuteNonQuery(); // 更新を実行
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
            public Decimal RegisteredDateTime { get; set; }
            public string RegisteredProgramID { get; set; }
            public string UpdatedUserCode { get; set; }
            public Decimal UpdatedDateTime { get; set; }
            public string UpdatedProgramID { get; set; }
            public string BaseRightNo { get; set; }       // NOBRRN
            public string BaseRightCategory { get; set; } // KBBRRN
            public Decimal LinkedDate { get; set; }      // DTBRRN
            public int Sequence { get; set; }              // NOSEQ
            public string Status { get; set; }             // STRENK
            public string Comment { get; set; }            // TXCOMT
            public string SourceFilePath { get; set; }     // VLPTBF
            public string DestinationFilePath { get; set; } // VLPTAF

            // List of detail DetailedExecutionBudgets (1:n relationship)
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

        public class BaseRightHistoryDetailEBOM : IBaseRightHistoryDetail
        {
            // Properties for BaseRight連携履歴明細ファイル(品目)
            public int DeleteFlag { get; set; }
            public string RegisteredUserCode { get; set; }
            public Decimal RegisteredDateTime { get; set; }
            public string RegisteredProgramID { get; set; }
            public string UpdatedUserCode { get; set; }
            public Decimal UpdatedDateTime { get; set; }
            public string UpdatedProgramID { get; set; }
            public string BaseRightNo { get; set; }             // NOBRRN
            public int EBOMLineNo { get; set; }                 // NPBRRH　
            public string CodeNumber { get; set; }  // TXBR01
            public string CodeBranchNumber { get; set; }  // TXBR02
            public string ParentItemNumber { get; set; }  // TXBR03
            public string BalloonNumber { get; set; }  // TXBR04
            public string Order { get; set; }  // TXBR05
            public string ChildItemNumber { get; set; }  // TXBR06
            public string PFlag { get; set; }  // TXBR07
            public string MFlag { get; set; }  // TXBR08
            public string Quantity { get; set; }  // TXBR09
            public string OrderQuantity { get; set; }  // TXBR10
            public string OrderInstructionNumber { get; set; }  // TXBR11
            public string SummaryOrderInstructionNumber { get; set; }  // TXBR12
            public string OrderRequestDate { get; set; }  // TXBR13
            public string OrderItemFlag { get; set; }  // TXBR14
            public string OrderCompletionFlag { get; set; }  // TXBR15
            public string ParentItemUniqueID { get; set; }  // TXBR16
            public string ItemUniqueID { get; set; }  // TXBR17
            public string HierarchyLevel { get; set; }  // TXBR18
            public string ItemNumber1 { get; set; }  // TXBR19
            public string ItemNumber2 { get; set; }  // TXBR20
            public string ItemNumber3 { get; set; }  // TXBR21
            public string ItemNumber4 { get; set; }  // TXBR22
            public string ItemNumber5 { get; set; }  // TXBR23
            public string ItemNumber6 { get; set; }  // TXBR24
            public string ItemNumber7 { get; set; }  // TXBR25
            public string ItemNumber8 { get; set; }  // TXBR26
            public string ItemNumber9 { get; set; }  // TXBR27
            public string ItemNumber10 { get; set; }  // TXBR28
            public string ExternalInspection { get; set; }  // TXBR29
            public string HighPressureGasCertified { get; set; }  // TXBR30
            public string FourTimesPressureResistance { get; set; }  // TXBR31
            public string WoodPattern { get; set; }  // TXBR32
            public string PaintProcedureExist { get; set; }  // TXBR33
            public string PaintingNotAllowedAtGasConnection { get; set; }  // TXBR34
            public string OilRestriction { get; set; }  // TXBR35
            public string WaterRestriction { get; set; }  // TXBR36
            public string ForNuclearUse { get; set; }  // TXBR37
            public string DesignChangeNotificationNumber { get; set; }  // TXBR38
            public string AssemblyDivision { get; set; }  // TXBR39
            public string Remarks { get; set; }  // TXBR40
            public string ManufacturerCode { get; set; }  // TXBR41
            public string RequirementForStandard2_02 { get; set; }  // TXBR42
            public string BudgetNumber { get; set; }  // TXBR43
            public string StrengthCalculationExist { get; set; }  // TXBR44
            public string PreDeliveryDocumentation { get; set; }  // TXBR45
            public string CompletionDocumentation { get; set; }  // TXBR46
            public string ItemUniqueAttribute19 { get; set; }  // TXBR47
            public string ItemUniqueAttribute20 { get; set; }  // TXBR48

            // Method to insert detail into the database
            public void Insert(OdbcConnection connection, string baseRightNo)
            {
                string detailQuery = @"
                                    INSERT 
                                    INTO FBBRE( 
                                          FGDELE                                    -- 削除フラグ
                                        , SPIUSR                                    -- 登録担当者コード
                                        , SPIDTM                                    -- 登録日時
                                        , SPIPGM                                    -- 登録プログラムID
                                        , SPUUSR                                    -- 更新担当者コード
                                        , SPUDTM                                    -- 更新日時
                                        , SPUPGM                                    -- 更新プログラムID
                                        , NOBRRN                                    -- BR連携No
                                        , NPBRRH                                    -- BR連携品目行No
                                        , TXBR01                                    -- コード番号
                                        , TXBR02                                    -- コード番号枝番
                                        , TXBR03                                    -- 親品目番号
                                        , TXBR04                                    -- 風船番号(照番)
                                        , TXBR05                                    -- 順序
                                        , TXBR06                                    -- 子品目番号
                                        , TXBR07                                    -- P
                                        , TXBR08                                    -- M
                                        , TXBR09                                    -- 員数
                                        , TXBR10                                    -- 手配数量
                                        , TXBR11                                    -- 手配指示番号
                                        , TXBR12                                    -- まとめ手配指示番号
                                        , TXBR13                                    -- 手配依頼日
                                        , TXBR14                                    -- 手配品目フラグ
                                        , TXBR15                                    -- 手配完了フラグ
                                        , TXBR16                                    -- 親品目構成固有ID
                                        , TXBR17                                    -- 品目構成固有ID
                                        , TXBR18                                    -- 階層レベル
                                        , TXBR19                                    -- 品目番号1
                                        , TXBR20                                    -- 品目番号2
                                        , TXBR22                                    -- 品目番号4
                                        , TXBR23                                    -- 品目番号5
                                        , TXBR24                                    -- 品目番号6
                                        , TXBR25                                    -- 品目番号7
                                        , TXBR26                                    -- 品目番号8
                                        , TXBR27                                    -- 品目番号9
                                        , TXBR28                                    -- 品目番号10
                                        , TXBR29                                    -- 外部検査
                                        , TXBR30                                    -- 高圧ガス認定品
                                        , TXBR31                                    -- 4倍耐圧
                                        , TXBR32                                    -- 木型
                                        , TXBR33                                    -- 塗装要領書有無
                                        , TXBR34                                    -- 接ガス部塗装不可
                                        , TXBR35                                    -- 禁油処理
                                        , TXBR36                                    -- 禁水処理
                                        , TXBR37                                    -- 原子力用
                                        , TXBR38                                    -- 設計変更通知番号
                                        , TXBR39                                    -- 組立区分
                                        , TXBR40                                    -- 備考
                                        , TXBR41                                    -- メーカーコード
                                        , TXBR42                                    -- 準2-02対応要否
                                        , TXBR43                                    -- 予算書番号
                                        , TXBR44                                    -- 強度計算書有無
                                        , TXBR45                                    -- 調達品の事前納品図
                                        , TXBR46                                    -- 調達品の完成時納入
                                        , TXBR47                                    -- コード番号構成固有
                                        , TXBR48                                    -- コード番号構成固有
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
                                        , ?                                   -- コード番号
                                        , ?                                   -- コード番号枝番
                                        , ?                                   -- 親品目番号
                                        , ?                                   -- 風船番号(照番)
                                        , ?                                   -- 順序
                                        , ?                                   -- 子品目番号
                                        , ?                                   -- P
                                        , ?                                   -- M
                                        , ?                                   -- 員数
                                        , ?                                   -- 手配数量
                                        , ?                                   -- 手配指示番号
                                        , ?                                   -- まとめ手配指示番号
                                        , ?                                   -- 手配依頼日
                                        , ?                                   -- 手配品目フラグ
                                        , ?                                   -- 手配完了フラグ
                                        , ?                                   -- 親品目構成固有ID
                                        , ?                                   -- 品目構成固有ID
                                        , ?                                   -- 階層レベル
                                        , ?                                   -- 品目番号1
                                        , ?                                   -- 品目番号2
                                        , ?                                   -- 品目番号4
                                        , ?                                   -- 品目番号5
                                        , ?                                   -- 品目番号6
                                        , ?                                   -- 品目番号7
                                        , ?                                   -- 品目番号8
                                        , ?                                   -- 品目番号9
                                        , ?                                   -- 品目番号10
                                        , ?                                   -- 外部検査
                                        , ?                                   -- 高圧ガス認定品
                                        , ?                                   -- 4倍耐圧
                                        , ?                                   -- 木型
                                        , ?                                   -- 塗装要領書有無
                                        , ?                                   -- 接ガス部塗装不可
                                        , ?                                   -- 禁油処理
                                        , ?                                   -- 禁水処理
                                        , ?                                   -- 原子力用
                                        , ?                                   -- 設計変更通知番号
                                        , ?                                   -- 組立区分
                                        , ?                                   -- 備考
                                        , ?                                   -- メーカーコード
                                        , ?                                   -- 準2-02対応要否
                                        , ?                                   -- 予算書番号
                                        , ?                                   -- 強度計算書有無
                                        , ?                                   -- 調達品の事前納品図
                                        , ?                                   -- 調達品の完成時納入
                                        , ?                                   -- コード番号構成固有
                                        , ?                                   -- コード番号構成固有
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
                    command.Parameters.AddWithValue("@NPBRRH", this.EBOMLineNo);
                    command.Parameters.AddWithValue("@TXBR01", this.CodeNumber);
                    command.Parameters.AddWithValue("@TXBR02", this.CodeBranchNumber);
                    command.Parameters.AddWithValue("@TXBR03", this.ParentItemNumber);
                    command.Parameters.AddWithValue("@TXBR04", this.BalloonNumber);
                    command.Parameters.AddWithValue("@TXBR05", this.Order);
                    command.Parameters.AddWithValue("@TXBR06", this.ChildItemNumber);
                    command.Parameters.AddWithValue("@TXBR07", this.PFlag);
                    command.Parameters.AddWithValue("@TXBR08", this.MFlag);
                    command.Parameters.AddWithValue("@TXBR09", this.Quantity);
                    command.Parameters.AddWithValue("@TXBR10", this.OrderQuantity);
                    command.Parameters.AddWithValue("@TXBR11", this.OrderInstructionNumber);
                    command.Parameters.AddWithValue("@TXBR12", this.SummaryOrderInstructionNumber);
                    command.Parameters.AddWithValue("@TXBR13", this.OrderRequestDate);
                    command.Parameters.AddWithValue("@TXBR14", this.OrderItemFlag);
                    command.Parameters.AddWithValue("@TXBR15", this.OrderCompletionFlag);
                    command.Parameters.AddWithValue("@TXBR16", this.ParentItemUniqueID);
                    command.Parameters.AddWithValue("@TXBR17", this.ItemUniqueID);
                    command.Parameters.AddWithValue("@TXBR18", this.HierarchyLevel);
                    command.Parameters.AddWithValue("@TXBR19", this.ItemNumber1);
                    command.Parameters.AddWithValue("@TXBR20", this.ItemNumber2);
                    command.Parameters.AddWithValue("@TXBR21", this.ItemNumber3);
                    command.Parameters.AddWithValue("@TXBR22", this.ItemNumber4);
                    command.Parameters.AddWithValue("@TXBR23", this.ItemNumber5);
                    command.Parameters.AddWithValue("@TXBR24", this.ItemNumber6);
                    command.Parameters.AddWithValue("@TXBR25", this.ItemNumber7);
                    command.Parameters.AddWithValue("@TXBR26", this.ItemNumber8);
                    command.Parameters.AddWithValue("@TXBR27", this.ItemNumber9);
                    command.Parameters.AddWithValue("@TXBR28", this.ItemNumber10);
                    command.Parameters.AddWithValue("@TXBR29", this.ExternalInspection);
                    command.Parameters.AddWithValue("@TXBR30", this.HighPressureGasCertified);
                    command.Parameters.AddWithValue("@TXBR31", this.FourTimesPressureResistance);
                    command.Parameters.AddWithValue("@TXBR32", this.WoodPattern);
                    command.Parameters.AddWithValue("@TXBR33", this.PaintProcedureExist);
                    command.Parameters.AddWithValue("@TXBR34", this.PaintingNotAllowedAtGasConnection);
                    command.Parameters.AddWithValue("@TXBR35", this.OilRestriction);
                    command.Parameters.AddWithValue("@TXBR36", this.WaterRestriction);
                    command.Parameters.AddWithValue("@TXBR37", this.ForNuclearUse);
                    command.Parameters.AddWithValue("@TXBR38", this.DesignChangeNotificationNumber);
                    command.Parameters.AddWithValue("@TXBR39", this.AssemblyDivision);
                    command.Parameters.AddWithValue("@TXBR40", this.Remarks);
                    command.Parameters.AddWithValue("@TXBR41", this.ManufacturerCode);
                    command.Parameters.AddWithValue("@TXBR42", this.RequirementForStandard2_02);
                    command.Parameters.AddWithValue("@TXBR43", this.BudgetNumber);
                    command.Parameters.AddWithValue("@TXBR44", this.StrengthCalculationExist);
                    command.Parameters.AddWithValue("@TXBR45", this.PreDeliveryDocumentation);
                    command.Parameters.AddWithValue("@TXBR46", this.CompletionDocumentation);
                    command.Parameters.AddWithValue("@TXBR47", this.ItemUniqueAttribute19);
                    command.Parameters.AddWithValue("@TXBR48", this.ItemUniqueAttribute20);

                    // Execute the Insert command for detail
                    command.ExecuteNonQuery();
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
                Decimal integrationDateTime,
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
                        using (var selectCommand = new OdbcCommand(selectQuery, connection, transaction))
                        {
                            // 取得したデータを読み込む
                            using (var reader = selectCommand.ExecuteReader())
                            {
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
                        using (var updateCommand = new OdbcCommand(updateQuery, connection, transaction))
                        {
                            updateCommand.Parameters.AddWithValue("?", updatedNoSeq);
                            updateCommand.Parameters.AddWithValue("?", registrationUserID);
                            updateCommand.Parameters.AddWithValue("?", integrationDateTime);
                            updateCommand.Parameters.AddWithValue("?", registrationProgramId);

                            int rowsAffected = updateCommand.ExecuteNonQuery();

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
        //
        // 構成展開処理用
        //
        static List<HierarchyItem> GetHierarchy(OdbcConnection connection ,OdbcTransaction transaction , string parentId)
        {
            var result = new List<HierarchyItem>();
            var children = new List<HierarchyItem>();

            // 初期親の子アイテムを取得
            string query = "SELECT CDOYKO, CDKOKO FROM FBZYE WHERE CDOYKO = ? ORDER BY NOFSEN ASC , NOSEQ ASC";
            using (OdbcCommand command = new OdbcCommand(query, connection))
            {
                command.Parameters.Add(new OdbcParameter("@CDOYKO", parentId));
                command.Transaction = transaction;
                using (OdbcDataReader reader = command.ExecuteReader())
                {
                    int level = 1;
                    while (reader.Read())
                    {
                        string childId = reader.GetString(1);
                        result.Add(new HierarchyItem { ParentID = parentId, ChildID = childId, Level = level });
                        // 再帰的に子アイテムを取得
                        children.AddRange(GetHierarchy(connection , transaction , childId , level + 1));
                    }
                }
            }

            result.AddRange(children);
            return result;
        }

        static List<HierarchyItem> GetHierarchy(OdbcConnection connection  , OdbcTransaction transaction , string parentId, int level)
        {
            var result = new List<HierarchyItem>();

            //using (OdbcConnection connection = new OdbcConnection(connectionString))
            //{
            //connection.Open();
            // 子アイテムを取得
            string query = "SELECT CDOYKO, CDKOKO FROM FBZYE WHERE CDOYKO = ? ORDER BY NOFSEN ASC , NOSEQ ASC";
            using (OdbcCommand command = new OdbcCommand(query, connection))
            {
                command.Parameters.Add(new OdbcParameter("CDOYKO", parentId));
                command.Transaction = transaction;
                using (OdbcDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string childId = reader.GetString(1);
                        result.Add(new HierarchyItem { ParentID = parentId, ChildID = childId, Level = level });
                        // 再帰的に子アイテムを取得
                        result.AddRange(GetHierarchy(connection , transaction, childId, level + 1));
                    }
                }
            }
            return result;
        }

        // 階層データを保持するクラス
        class HierarchyItem
        {
            public string ParentID { get; set; }
            public string ChildID { get; set; }
            public int Level { get; set; }
        }
    }
}
