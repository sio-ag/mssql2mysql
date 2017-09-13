<?php
/*
 * SOURCE: MS SQL
 */
define('MSSQL_HOST','192.168.33.1');
define('MSSQL_USER','SA');
define('MSSQL_PASSWORD','<<CHANGE_ME>>');
define('MSSQL_DATABASE','wt3datHS');

/*
 * DESTINATION: MySQL
 */
define('MYSQL_HOST', '127.0.0.1');
define('MYSQL_USER', 'root');
define('MYSQL_PASSWORD', '1234');
define('MYSQL_DATABASE', 'hauser');

/*
 * STOP EDITING!
 */

set_time_limit(0);

function addQuote($string)
{
	return "'".$string."'";
}

function addTilde($string)
{
	return "`".$string."`";
}

// Connect MS SQL
$mssql_db = sqlsrv_connect(MSSQL_HOST, ['Uid' => MSSQL_USER, 'PWD' => MSSQL_PASSWORD, 'Database' => MSSQL_DATABASE, 'CharacterSet' => 'UTF-8']) or die("Couldn't connect to SQL Server on '".MSSQL_HOST."'' user '".MSSQL_USER."'\n");
echo "=> Connected to Source MS SQL Server on '".MSSQL_HOST."'\n";

// Connect to MySQL
$mysqli = new mysqli(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE) or
die("Couldn't connect to MySQL on '".MYSQL_HOST."'' user '".MYSQL_USER."'\n");
echo "\n=> Connected to Destination MySQL Server on ".MYSQL_HOST."\n";

$mssql_tables = array();

// Get MS SQL tables
$sql = "SELECT * FROM sys.Tables;";
$res = sqlsrv_query($mssql_db, $sql);
echo "\n=> Getting tables..\n";
while ($row = sqlsrv_fetch_array($res))
{
	array_push($mssql_tables, $row['name']);
	//echo ($row['name'])."\n";
}
echo "==> Found ". number_format(count($mssql_tables),0,',','.') ." tables\n\n";

$blacklisted = [
    'wv_Delegationen',
    'wv_Delegationen_Hist',
    'Datei_Historie',
    'Itinerary',
    'SR_EQ',
    'PLZ',
    'Buchungen_BuBeTexte',
    'WV_System',
    'TourInfo_DDSrc',
    'BS_Archiv',
    'BS_Umsatz',
    'SR_Zimmer_Detail',
    'BS_Umsatz_lfd',
    'WV_System_Hist',
    'Reiseteilnehmer_del',
    'BSPaidProv',
    'KFB_WEB',
    'Buchhaltung_Prov',
    'Rechnung_Text_ZB',
    'SR_Kalk_Paxvariante',
    'ZahlungenImp',
    'Buchhaltung_FiBu',
    'tblKalk',
    'Adressen_deleted',
    'Buchhaltung',
    'BSPaidProv_gebucht',
    'TextLeistungen',
    'SR_Kalk_Preis_pro',
    'KFB_logMails',
    'KFB_Log',
    'KFB_Job',
    'SR_Publizieren',
    'F_VK_Preise',
    'WV_AutoHistorie',
    'BerichtsLog',
    'Adressen_Hist',
    'Buchungssaetze_ProMain',
    'Kontingente',
    'KontingentTage',
    'Personenkonten',
    'SR_Tour_EQ',
];

// Get Table Structures
if (!empty($mssql_tables))
{
    $i = count($mssql_tables);

	foreach ($mssql_tables as $table)
	{
        echo '====> '.$i--.'. '.$table."\n";

        if (in_array($table, $blacklisted)) {
            echo "=====> BLACKLISTED\n\n";
            continue;
        }

		echo "=====> Getting info table ".$table." from SQL Server\n";

		$sql = "select * from information_schema.columns where table_name = '".$table."'";
		$res = sqlsrv_query($mssql_db, $sql);

		if ($res)
		{
			$mssql_tables[$table] = array();

			$mysql = "DROP TABLE IF EXISTS `".$table."`";
			$mysqli->query($mysql);
			$mysql = "CREATE TABLE `".$table."`";
			$strctsql = $fields = array();

			while ($row = sqlsrv_fetch_array($res))
			{
				//print_r($row); echo "\n";
				array_push($mssql_tables[$table], $row);

				switch ($row['DATA_TYPE']) {
					case 'bit':
					case 'tinyint':
					case 'smallint':
					case 'int':
					case 'bigint':
						$data_type = $row['DATA_TYPE'].(!empty($row['NUMERIC_PRECISION']) ? '('.$row['NUMERIC_PRECISION'].')' : '' );
						break;

					case 'money':
						$data_type = 'decimal(19,4)';
						break;
					case 'smallmoney':
						$data_type = 'decimal(10,4)';
						break;

					case 'real':
                        $data_type =
                            'float'.
                            (!empty($row['NUMERIC_PRECISION']) ?
                                '('.
                                $row['NUMERIC_PRECISION'].
                                (!empty($row['NUMERIC_SCALE']) ? ','.$row['NUMERIC_SCALE'] : '').
                                ')' : '');
                        break;

					case 'float':
					case 'decimal':
					case 'numeric':
						$data_type = $row['DATA_TYPE'].(!empty($row['NUMERIC_PRECISION']) ? '('.$row['NUMERIC_PRECISION'].(!empty($row['NUMERIC_SCALE']) ? ','.$row['NUMERIC_SCALE'] : '').')' : '' );
						break;

					case 'date':
					case 'datetime':
					case 'timestamp':
					case 'time':
						$data_type = $row['DATA_TYPE'];
					case 'datetime2':
					case 'datetimeoffset':
					case 'smalldatetime':
						$data_type = 'datetime';
						break;

					case 'nchar':
					case 'char':
                    case 'nvarchar':
                    case 'varchar':
					    $max = (int)$row['CHARACTER_MAXIMUM_LENGTH'];
					    if (1 > $max || 255 < $max) {
					        $data_type = 'text';
                        } else {
					        $data_type = (false !== strpos($row['DATA_TYPE'], 'var') ? 'var' : '').'char('.$max.')';
                        }
                        break;
					case 'ntext':
					case 'text':
						$data_type = 'text';
						break;

					case 'binary':
					case 'varbinary':
						$data_type = $data_type = $row['DATA_TYPE'];
					case 'image':
						$data_type = 'blob';
						break;

					case 'uniqueidentifier':
						$data_type = 'char(36)';//'CHAR(36) NOT NULL';
						break;

					case 'cursor':
					case 'hierarchyid':
					case 'sql_variant':
					case 'table':
					case 'xml':
					default:
						$data_type = false;
						break;
				}

				if (!empty($data_type))
				{
					$ssql = "`".$row['COLUMN_NAME']."` ".$data_type." ".($row['IS_NULLABLE'] == 'YES' ? 'NULL' : 'NOT NULL');
					if (preg_match('/(ID|Id|Nr)$/', $row['COLUMN_NAME'])) {
					    $ssql .= ',INDEX `'.$row['COLUMN_NAME'].'` (`'.$row['COLUMN_NAME'].'`)';
                    }
					array_push($strctsql, $ssql);
					array_push($fields, $row['COLUMN_NAME']);
				}

			}

			echo "=====> Getting data from table ".$table." on SQL Server\n";
			$sql = "SELECT * FROM ".$table;
			$qres = sqlsrv_query($mssql_db, $sql, [], ['Scrollable' => 'static']);
			$numrow = sqlsrv_num_rows($qres);
			echo "======> Found ".number_format($numrow,0,',','.')." rows\n";

			if (0 === $numrow) {
                echo "=======> Skipping.\n\n";
			    continue;
            }

            $mysql .= "(".implode(',', $strctsql).") DEFAULT CHARACTER SET = 'utf8';";
            echo "======> Creating table ".$table." on MySQL... ";
            $q = $mysqli->query($mysql);
            echo (($q) ? 'Success':'Failed!'."\n".$mysql."\n")."\n";

            if ($qres)
			{
				echo "=====> Inserting to table ".$table." on MySQL\n";
				$numdata = 0;
				if (!empty($fields))
				{
					$sfield = array_map('addTilde', $fields);
					while ($qrow = sqlsrv_fetch_array($qres))
					{
						$datas = array();
						foreach ($fields as $field)
						{
							$ddata = (!empty($qrow[$field])) ? $qrow[$field] : '';
							if ($ddata instanceof DateTimeInterface) {
								$ddata = $ddata->format('c');
							}
							array_push($datas,"'".$mysqli->real_escape_string(utf8_decode($ddata))."'");
						}

						if (!empty($datas))
						{
							//$datas = array_map('addQuote', $datas);
							//$fields =
							$mysql = "INSERT INTO `".$table."` (".implode(',',$sfield).") VALUES (".implode(',',$datas).");";
							//$mysql = mysql_real_escape_string($mysql);
							//echo $mysql."\n";
							$q = $mysqli->query($mysql);
							$numdata += ($q ? 1 : 0 );
						}
					}
				}
				echo "======> ".number_format($numdata,0,',','.')." data inserted\n\n";

				if (0 == $numdata) {
				    $mysqli->query('DROP TABLE `'.$table.'`');
                }
			}
		}
	}

}

echo "Done!\n";

sqlsrv_close($mssql_db);
$mysqli->close();
