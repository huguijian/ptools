<?php
namespace Ptools\Database;

/**
 * Created by PhpStorm.
 * User: huguijian
 * Date: 2018/12/27
 * Time: 9:21 AM
 */
class DB {

    /**
     * 普通表
     *
     * @var int
     */
    const TABLE_NORMAL = 1;

    /**
     * hash 表crc32
     *
     * @var int
     */
    const TABLE_CRC32  = 2;

    /**
     * hash 表md5
     *
     * @var int
     */
    const TABLE_MD5    = 3;

    /**
     * pdo 连接对象
     *
     * @var null
     */
    private $pdo      = null;

    private $stmt     = null;


    /**
     * 数据库配置
     *
     * @var array
     */
    protected $dbConfig    = array ();

    /**
     * 表配置
     *
     * @var array
     */
    protected $tableConfig = array();

    /**
     * 真实表
     *
     * @var null
     */
    public   $realTable  = null;


    /**
     * 是否开启事务
     *
     * @var bool
     */
    private   $transaction = false;

    /**
     * last error
     *
     * @var array
     */
    private   $lastError     = [];

    /**
     * last error code
     *
     * @var string
     */
    private   $lastErrorCode = "";
    /**
     * 实例对象
     *
     * @var null
     */
    protected static $_instance = null;


    protected $fetchType = \PDO::FETCH_ASSOC;

    private $where   = "";

    private $limit   = "";

    private $order   = "";

    private $lastSql = "";

    private $bindVals = array();
    /**
     * DB constructor.
     * @param array $dbConfig
     */
    public function __construct($dbConfig = array())
    {

        if (!empty($dbConfig)) {

            $this->dbConfig = $dbConfig;
        }
        self::connect();
    }

    /**
     * 获取Model实例
     * @param array $dbConfig
     * @return null|static
     */
    public static function getInstance($dbConfig = array())
    {
        $link = md5(implode(",",$dbConfig));
        if (!isset(self::$_instance[$link]) || self::$_instance[$link]==null) {

            self::$_instance[$link] = new static($dbConfig);
        }

        return self::$_instance[$link];
    }


    /**
     * pdo mysql链接
     */
    private function connect()
    {
        $pdo = null;
        $dsn = $this->dbConfig["dbType"].":"."host=".$this->dbConfig["host"].";"."dbname=".$this->dbConfig["dbName"].";"."port=".$this->dbConfig["port"];
        try {
            $pdo = new \PDO($dsn,$this->dbConfig["user"],$this->dbConfig["pw"],array(\PDO::MYSQL_ATTR_INIT_COMMAND => "set names utf8,sql_mode=''"));
        } catch (\PDOException $e) {

            throw new \PDOException("数据库连接错误:".$e->getMessage());

        }

        $this->pdo = $pdo;
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    public function setTable($table)
    {
        if (is_string($table)) {
            $this->realTable = $table;

        } else if (is_array($table)) {
            $this->tableConfig = $table;
        }
        return $this;
    }

    /**
     * 插入数据
     * @param array $data
     * @return int
     */
    public function insert( Array $data)
    {

        $table = $this->realTable;

        $_addFields   = array();
        $_addValues   = array();
        $insertFields = array();
        foreach ($data as $_key=>$_value) {
            $_addFields[] = $_key;
            $_addValues[] = $_value;
            $insertFields[":insert_$_key"] = $_value;
        }

        $this->bindVals["insert_field"] = $insertFields;
        $_addFields  = implode(',', $_addFields);
        $_bindValues = implode(",", array_keys($insertFields));
        $sql = "INSERT INTO ". $table . " ($_addFields) VALUES ($_bindValues)";
        if (false!== $count = $this->execute($sql,true)->rowCount()) {
            return !empty($this->pdo->lastInsertId()) ? $this->pdo->lastInsertId() : $count;
        }
    }


    /**
     * 更新数据
     * @param array $data
     * @return mixed
     */
    public function update($data=array())
    {
        $table = $this->realTable;
        $setData = "";
        foreach ($data as $key=>$val) {
            $setData .= " $key='$val',";
        }
        $setData = substr($setData, 0, -1);

        $sql = "UPDATE $table SET $setData {$this->where}";
        return $this->execute($sql,true)->rowCount();
    }

    /**
     * 删除数据
     * @return mixed
     */
    public function delete()
    {
        $table = $this->realTable;

        $sql   = "DELETE FROM $table {$this->where}";
        return $this->execute($sql,true)->rowCount();
    }

    /**
     * 查询表记录
     * @param array $fields
     * @return mixed
     */
    public function select($fields=array())
    {
        $table = $this->realTable;

        $selectFields = !empty($fields) ? implode(',', $fields) : "*";

        $sql = "SELECT $selectFields FROM $table {$this->where}  {$this->order} {$this->limit}";

        $stmt = $this->execute($sql,true);

        if (substr($this->limit,-3)==='one') {
            $result = $stmt->fetch($this->fetchType);
        } else {
            $result = $stmt->fetchAll($this->fetchType);
        }

        return $result;
    }

    /**
     * 查询单条
     * @param array $fields
     * @return mixed
     */
    public function find($fields=array())
    {

        return $this->limit(1)->select($fields);
    }

    /**
     * 获取数据表中的记录数
     * @param string $field
     * @return mixed
     */
    public function count($field="*")
    {
        $table = $this->realTable;
        $sql = "SELECT COUNT($field) as count FROM $table {$this->where}";
        $stmt = $this->execute($sql,true);
        $result = $stmt->fetch($this->fetchType);
        return $result["count"];
    }

    /**
     * 查询条数
     * @param string $limit
     * @return $this
     */
    public function limit($limit = "")
    {
        if (is_numeric($limit)) {
            if ($limit===1) {
                $this->bindVals["limit"] = array(
                    ":limit_one" => $limit
                );
                $limit = " LIMIT :limit_one";
            } else {
                $this->bindVals["limit"] = array(
                    ":limit" => $limit
                );
                $limit = " LIMIT :limit";
            }

        } else if (is_array($limit)) {

            $this->bindVals["limit"] = array(
                ":limit_start" => $limit[0],
                ":limit_end"   => $limit[1]
            );

            $bindLimit = array_keys($this->bindVals["limit"]);
            $limit = " LIMIT ".implode(",",$bindLimit);
        }
        $this->limit = $limit;
        return $this;
    }

    /**
     * 排序
     * @param string $order
     * @return $this
     */
    public function order($order = "")
    {
        $order = " ORDER BY $order";
        $this->order = $order;
        return $this;
    }

    /**
     * 更新指定字段累加
     * @param $field
     * @param int $val
     * @return mixed
     */
    public function setInc($field, $val=0)
    {
        $table = $this->realTable;
        $sql   = "UPDATE $table SET $field=$field+$val {$this->where}";
        return $this->execute($sql,true)->rowCount();

    }

    /**
     * 更新指定字段累减
     * @param $field
     * @param int $val
     * @return mixed
     */
    public function setDec($field, $val=0)
    {

        $table = $this->realTable;
        $sql   = "UPDATE $table SET $field=$field-$val {$this->where}";
        return $this->execute($sql,true)->rowCount();
    }

    /**
     * sql条件构建
     * @param array $map
     * @return $this
     */
    public function where($map = array())
    {
        $where = "WHERE ";
        $bindVals = array();
        foreach ($map as $field=>$val) {
            $bindField = ":where_".$field;

            if (!is_array($val)) {

                $bindVals[$bindField] = $val;
                $where .= " $field=$bindField AND ";

            } else {
                switch ($val[0]) {
                    case "in" :
                        if (is_array($val[1])) {
                            foreach ($val[1] as $key=>$item) {

                                $bindVals[$bindField.$key."_in"] = $item;
                            }
                            $bindField = implode(",",array_keys($bindVals));
                        }
                        $where .= "$field IN ($bindField) AND ";

                        break;

                    case "not in" :
                        if (is_array($val[1])) {
                            foreach ($val[1] as $key=>$item) {

                                $bindVals[$bindField.$key."_not_in"] = $item;
                            }
                            $bindField = implode(",",array_keys($bindVals));
                        }
                        $where .= "$field NOT IN ($bindField) AND ";
                        break;

                    case "between":
                        $btField0 =  "$bindField"."0_between";
                        $btField1 =  "$bindField"."1_between";
                        $bindField = array(
                            $btField0,
                            $btField1
                        );
                        $where .= "$field BETWEEN ".implode(" AND ",$bindField)." AND ";
                        $bindVals[$btField0] = $val[1][0];
                        $bindVals[$btField1] = $val[1][1];
                        break;

                    case "like":
                        $bindField = $bindField."_like";
                        $where .= "$field LIKE $bindField AND ";
                        $bindVals[$bindField] = $val[1];
                        break;

                    default :
                        $bindField = $bindField."_default";
                        $where .= "$field $val[0] $bindField AND ";
                        $bindVals[$bindField] = $val[1];
                        break;
                }

            }
        }
        $this->bindVals["where"] = $bindVals;
        $where = substr($where,0, -4);
        $this->where = $where;
        return $this;
    }

    /**
     * 解析bind val
     * @return null
     */
    private function parseBindVal()
    {
        //where bind val
        if (isset($this->bindVals["where"])) {
            foreach ($this->bindVals["where"] as $key=>$val) {

                    $this->stmt->bindValue($key, $val, self::getBindValType($val));

            }
            unset($this->bindVals["where"]);
            $this->where = "";
        }


        //limit bind val
        if (isset($this->bindVals["limit"])) {

            foreach ($this->bindVals["limit"] as $key=>$val) {
                $this->stmt->bindValue($key,$val,self::getBindValType($val));
            }
            unset($this->bindVals["limit"]);
            $this->limit = "";
        }


        //insert field bind val
        if (isset($this->bindVals["insert_field"])) {
            foreach ($this->bindVals["insert_field"] as $key=>$val) {

                $this->stmt->bindValue($key,$val,self::getBindValType($val));
            }
            unset($this->bindVals["insert_field"]);
        }

        $this->realTable = "";
        $this->order     = "";
        return $this->stmt;
    }

    /**
     * 获取bind val Type
     * @param $val
     * @return int|null
     */
    private static function getBindValType($val)
    {
        $valType = null;
        if (is_int($val)) {
            $valType = \PDO::PARAM_INT;

        } else if(is_string($val) || is_float($val)) {
            $valType = \PDO::PARAM_STR;

        } else if(is_bool($val)) {
            $valType = \PDO::PARAM_BOOL;

        } else if(is_null($val)) {
            $valType = \PDO::PARAM_NULL;

        }
        return $valType;
    }

    /**
     * 获取最近一条执行的sql
     * @return string
     */
    public function getLastSql()
    {
        return $this->lastSql;
    }

    /**
     * 获取crc32 路由表
     * @param string $preTable
     * @param string $string
     * @param int $tableCount
     * @return string
     */
    private function getTableByCrc32($preTable="",$string="",$tableCount=0)
    {
        $crc32 = sprintf('%u', crc32(md5($string)));
        if ($crc32 > 2147483647)  // sprintf u for 64 & 32 bit
        {
            $crc32 -= 4294967296;
        }

        return $preTable.abs($crc32) % $tableCount;
    }


    /**
     * 获取 hash md5 路由表
     * @param string $preTable
     * @param string $string
     * @param int $tableCount
     * @return string
     */
    private function getTableByMd5($preTable="",$string="",$tableCount=16)
    {
        if (!in_array($tableCount,array(16,256))) {
            exit("表数量不正确\n");
        }
        $tableSuffix = "";
        switch($tableCount) {
            case 16 :
                $tableSuffix = substr(md5($string),-1);
                break;
            case 256 :
                $tableSuffix = substr(md5($string),-2);
                break;
            default:
                break;
        }

        return $preTable.$tableSuffix;

    }


    /**
     * 获取表
     * @param string $hashId
     * @return $this
     */
    public function getTable($hashId="")
    {
        $table = null;

        if ($this->tableConfig["tableType"] == self::TABLE_NORMAL) {

            $table = $this->tableConfig["tableName"];

        } else if ($this->tableConfig["tableType"] == self::TABLE_CRC32) {

            $table = $this->getTableByCrc32($this->tableConfig["tableName"],$hashId,$this->tableConfig["tableNum"]);


        } else if ($this->tableConfig["tableType"] == self::TABLE_MD5) {

            $table = $this->getTableByMd5($this->tableConfig["tableName"],$hashId,$this->tableConfig["tableNum"]);

        }

        $this->realTable = $table;

        return $this;
    }


    /**
     * 事务回滚
     * @return mixed
     */
    public function rollback()
    {
        $result = $this->pdo->rollback();
        $this->transaction = false;
        return $result;
    }


    /**
     * 检测事务状态
     */
    public function checkTransactionStatus()
    {
        if (!$this->transaction) {

            return;
        }
        $this->rollback();
    }

    /**
     * 开启事务
     */
    public function beginTransaction()
    {
        $this->pdo->setAttribute(PDO::ATTR_AUTOCOMMIT,0);
        $this->pdo->beginTransaction();
        $this->transaction = true;
        register_shutdown_function([$this,"checkTransactionStatus"]);

    }

    /**
     * 提交事务
     * @return mixed
     */
    public function commit()
    {
        $result = $this->pdo->commit();
        $this->pdo->setAttribute(PDO::ATTR_AUTOCOMMIT,1);
        $this->transaction = false;
        return $result;
    }

    /**
     * 获取db错误信息
     * @return array
     */
    public function getLastError()
    {
        return $this->lastError;
    }

    /**
     * 获取db错误码
     * @return string
     */
    public function getLastErrorCode()
    {
        return $this->lastErrorCode;
    }


    /**
     * 原生sql查询
     * @param $sql
     * @return array
     */
    public function query($sql)
    {
        try{
            $this->stmt = $stmt = $this->pdo->prepare($sql);

            if (!$stmt instanceof \PDOStatement) {
                $this->lastErrorCode = $this->pdo->errorCode();
                $this->lastError     = $this->pdo->errorInfo();
            }
            $this->lastSql = $sql;
            $this->stmt->execute();
        }catch (\PDOException $e) {

            throw new \PDOException("SQL语句:".$sql."错误信息:".$e->getMessage());
        }

        return $this->stmt->fetchAll($this->fetchType);
    }

    /**
     * 执行sql
     * @param $sql
     * @return mixed
     */
    public function execute($sql,$flag = false)
    {
        try{

            $this->stmt = $stmt = $this->pdo->prepare($sql);
            if (!$stmt instanceof \PDOStatement) {
                $this->lastErrorCode = $this->pdo->errorCode();
                $this->lastError     = $this->pdo->errorInfo();
            }

            if (!empty($this->bindVals)) {
                $this->parseBindVal();
            }

            //$this->stmt->debugDumpParams();
            $this->lastSql = $sql;
            $this->stmt->execute();
        }catch (\PDOException $e) {
            throw new \PDOException("SQL语句:".$sql."错误信息:".$e->getMessage());
        }

        return $flag === true ? $this->stmt : $this->stmt->rowCount();
    }
    /*
     * 字段求和
     * @param $field
     * @return mixed
     */
    public function sum($field)
    {
        $table = $this->realTable;
        $sql = "SELECT SUM($field) as val FROM $table {$this->where}";
        $stmt = $this->execute($sql,true);
        $result = $stmt->fetch($this->fetchType);
        return $result["val"];
    }

}
