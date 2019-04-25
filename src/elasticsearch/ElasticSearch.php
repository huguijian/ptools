<?php
namespace Ptools\Elasticsearch;
use Elasticsearch\ClientBuilder;

class ElasticSearch {

    /**
    'host' => '172.16.16.108',
    'port' => '9200',
    'scheme' => 'https',
    'user' => 'elastic',
    'pass' => 'Pzh6537projectx'
     */
    protected $hosts = [];

    protected $indexType = ["index"=>null,"type"=>null];

    protected $query = [];

    protected static $_instance = null;

    protected $Client  = null;

    public function __construct()
    {
        $this->hosts['host'] = env('ES_HOST');
        $this->hosts['port'] = env('ES_PORT');
        $clientBuilder = ClientBuilder::create()->setHosts($this->hosts)->build();

        $this->Client = $clientBuilder;
    }

    /**
     * 获取实例
     * @return null|static
     */
    public static function getInstance()
    {
        if(!isset(self::$_instance)) {
            self::$_instance = new static();
        }

        return self::$_instance;
    }

    /**
     * 设置index\type
     * @param $index
     * @param $type
     */
    public function setIndexType($index,$type)
    {
        if(!empty($index)) {

            $this->indexType["index"] = $index;

        }

        if(!empty($type)) {

            $this->indexType["type"] = $type;

        }
        return $this;
    }
    /**
     * 创建一个索引(index\setting\mapping)
     * @param string $index
     * @param array $settings
     * @param array $maps
     * @param null|string $type
     * @return array|bool
     * @throws \Exception
     */
    public function createIndex( array $maps, array $settings = [])
    {
        $type = $this->indexType["type"];
        if (empty($this->indexType["type"])) {
            $type = $this->indexType["index"];
        }

        if ($this->exists($this->indexType["index"])) {
            return true;
        }

        $params          = [];
        $params['index'] = $this->indexType["index"];
        $params['body']  = [
            'mappings' => [
                $type => $maps,
            ],
        ];

        !empty($settings) && $params["body"]["setting"] = $settings;

        try {
            $ret = $this->Client->indices()->create($params);
        } catch (\Exception $e) {
           throw new \Exception($e->getMessage());
        }
        return $ret;

    }


    /**
     * 删除一个索引
     * @param $index
     * @return array
     */
    public function delIndex()
    {
        $deleteParams = [
            'index' => $this->indexType["index"],
        ];
        $response = $this->Client->indices()->delete($deleteParams);
        return $response;
    }

    /**
     * 判断索引是否存在
     * @param string $index
     * @return bool
     * @throws \Exception
     */
    public function exists() {
        $params = [
            'index' => $this->indexType["index"],
        ];
        try {
            return $this->Client->indices()->exists($params);
        } catch (\Exception $e) {
            throw new \Exception($e->getMessage());
        }
    }

    /**
     * 查看mapping配置
     * @param $index
     * @param string $type
     * @return array|bool
     * @throws \Exception
     */
    public function getMapping() {

        $params = [];

        if (!empty($this->indexType["index"])) {
            $params = [
                'index' => $this->indexType["index"],
            ];
        }

        if (!empty($this->indexType["type"])) {
            $params = array_merge((array)$params,["type"=>$this->indexType["type"]]);
        }
        if (!$this->exists($this->indexType["index"])) {
            return false;
        }
        try {
            return $this->Client->indices()->getMapping($params);
        } catch (\Exception $e) {
           throw new \Exception($e->getMessage());
        }
    }


    /**
     * 更改或设置一个mapping
     * @param $index
     * @param $type
     * @param $maps
     * @return array
     */
    public function putMapping($maps)
    {
        // Set the index and type
        $params = [
            'index' => $this->indexType["index"],
            'type'  => $this->indexType["type"],
            'body'  => [
                $this->indexType["type"] => $maps
            ]
        ];

        //Update the index mapping
        return $this->Client->indices()->putMapping($params);
    }

    /**
     * 索引设置
     * @param array $config
     * @return array
     */
    public function putSettings($config=array())
    {
        $params = [
            'index' => $this->indexType["index"],
            'body' => [
                'settings' => $config
            ]
        ];
        $response = $this->Client->indices()->putSettings($params);
        return $response;
    }

    /**
     * 获取索引设置
     * @param $index
     * @return array
     */
    public function getSettings()
    {
        $params = [
            'index' => $this->indexType["index"]
        ];
        return $this->Client->indices()->getSettings($params);
    }


    /**
     * 索引一个文档
     * @param string $index
     * @param array $body
     * @param null|string $id
     * @param null|string $type
     * @return array|bool
     * @throws \Exception
     */
    public function createDoc( array $body,  ? string $id)
    {

        $type = $this->indexType["type"];
        if (empty($this->indexType["type"])) {
            $type = $this->indexType["index"];
        }

        if (!$this->exists($this->indexType["index"])) {
            return false;
        }
        $params          = [];
        $params['index'] = $this->indexType["index"];
        $params['type']  = $type;
        if (!empty($id)) {
            $params['id'] = $id;
        }
        $params['body'] = $body;

        try {
            return $this->Client->index($params);
        } catch (\Exception $e) {
            throw new \Exception($e->getMessage());
        }
    }

    /**
     * 删除一个文档
     * @param int $id
     * @return array
     */
    public function delDoc($id=0)
    {

        $params["id"] = $id;
        $params = array_merge($this->indexType,["id"=>$id]);

        $response = $this->Client->delete($params);
        return $response;
    }

    /**
     * 获取一个文档
     * @param $id
     * @return array
     */
    public function getDoc($id)
    {

        $params = [
            'id' => $id,
            'client' => [ 'ignore' => 404 ]
        ];
        $params = array_merge($this->indexType,$params);

        $response = $this->Client->get($params);

        return $response;
    }

    /**
     * 更新\添加文档
     * @param $id
     * @param $body
     * @return array
     */
    public function upDoc($id,$body)
    {
        $params = [
            'id' => $id,
            'body' => $body
        ];
        $params = array_merge($this->indexType,$params);

        return $response = $this->Client->update($params);
    }

    /**
     * 等于条件
     * @param $where
     * @return $this
     */
    public function where($where)
    {
        foreach($where as $key=>$item){
            if(false!==strpos($key,"filter")) {
                foreach ($item as $field=>$val) {
                    $bool[$key][] = ['term' => [$field => $val]];
                }
            }else if(in_array($key,['must','should','must_not'])) {
                foreach ($item as $field=>$val) {
                    $bool[$key][] = ['match' => [$field => $val]];
                }
            }
        }

        $this->query = [
            'query' =>[
                'bool' => $bool,
            ],
        ];

        return $this;
    }

    /**
     * 范围条件
     * @param array $between
     * @return $this
     */
    public function between($between=array())
    {
        $this->query["query"]["bool"]["filter"][] =  ["range"=>$between];
        return $this;
    }

    /**
     * 分组条件
     * @param $field
     * @param $subagg
     * @return $this
     */
    public function group($field,$subagg)
    {
        $groupName = "group_".$this->indexType["type"];
        $this->query["aggs"] = [
            $groupName => [
                'terms' => [
                    'field' => $field,
                    'size'  => 2147483647,
                ],
                'aggs' => [
                    'group_tag_name' => [
                        'terms' => ['field' => $subagg]
                    ],
                ]
            ],
        ];

        return $this;
    }

    /**
     * 搜索文档
     * @return array
     */
    public function search()
    {
        $body["body"] = $this->query;
        $params = array_merge($this->indexType,$body);
        //var_dump($params);exit;
        return $this->Client->search($params);
    }


}