<?php
namespace Ptools\Auth;
class Auth{

    //默认配置
    protected $_config = array(
        'AUTH_ON' => true, //认证开关
        'AUTH_TYPE' => 1, // 认证方式，1为时时认证；2为登录认证。
        'AUTH_GROUP' => 't_user_group_auth', //用户组所有权限表
        'AUTH_GROUP_ACCESS' => 't_user_group', //用户对应组表
        'AUTH_RULE' => 't_node', //节点表
        'AUTH_USER' => 't_user'//用户信息表
    );

    public function __construct() {

    }

    //获得权限$name 可以是字符串或数组或逗号分割， uid为 认证的用户id， $or 是否为or关系，为true是， name为数组，只要数组中有一个条件通过则通过，如果为false需要全部条件通过。
    public function check($name, $uid, $relation='or') {
        if (!$this->_config['AUTH_ON'])
            return true;
        $authList = $this->getAuthList($uid);

        if (is_string($name)) {
            if (strpos($name, ',') !== false) {
                $name = explode(',', $name);
            } else {
                $name = array($name);
            }
        }
        $list = array(); //有权限的name

        foreach ($authList as $val) {
            if (in_array($val, $name))
                $list[] = $val;
        }
        if ($relation=='or' and !empty($list)) {
            return true;
        }
        $diff = array_diff($name, $list);
        if ($relation=='and' and empty($diff)) {
            return true;
        }
        return false;
    }

    //获得用户组，外部也可以调用
    public function getGroups($uid) {
        static $groups = array();
        if (isset($groups[$uid]))
            return $groups[$uid];

        $sql = "SELECT * FROM {$this->_config['AUTH_GROUP_ACCESS']} a INNER JOIN {$this->_config['AUTH_GROUP']} g on a.group_id=g.group_id WHERE a.user_id='$uid' and g.status='1'";

        $user_groups = app('db')->select($sql);
        $user_groups = array_map('get_object_vars', $user_groups);
        $groups[$uid]=$user_groups?$user_groups:array();
        return $groups[$uid];
    }

    //获得权限列表
    protected function getAuthList($uid) {
        static $_authList = array();
        if (isset($_authList[$uid])) {
            return $_authList[$uid];
        }

        //读取用户所属用户组
        $groups = $this->getGroups($uid);
        $ids = array();
        foreach ($groups as $g) {
            $ids = array_merge($ids, explode(',', trim($g['rules'], ',')));
        }
        $ids = array_unique($ids);
        if (empty($ids)) {
            $_authList[$uid] = array();
            return array();
        }

        $ids_str = implode(",",$ids);
        //读取用户组所有权限规则
        $sql = "SELECT * FROM {$this->_config['AUTH_RULE']} WHERE node_id IN($ids_str) AND status=1";
        $rules = app('db')->select($sql);
        $rules = array_map('get_object_vars', $rules);

        //循环规则，判断结果。
        $authList = array();
        foreach ($rules as $r) {
            if (!empty($r['condition'])) {
                //条件验证
                $user = $this->getUserInfo($uid);
                $command = preg_replace('/\{(\w*?)\}/', '$user[\'\\1\']', $r['condition']);
                //dump($command);//debug
                @(eval('$condition=(' . $command . ');'));
                if ($condition) {
                    $authList[] = strtolower($r['router']);
                }
            } else {
                //存在就通过
                $authList[] = strtolower($r['router']);
            }
        }
        $_authList[$uid] = $authList;

        return $authList;
    }
    //获得用户资料,根据自己的情况读取数据库
    protected function getUserInfo($uid) {
        static $userinfo=array();
        if(!isset($userinfo[$uid])){
            $uinfo=app('db')->select("SELECT * FROM {$this->_config['AUTH_USER']} WHERE user_id=$uid");
            $uinfo = array_map('get_object_vars', $uinfo);
            $uinfo = $uinfo[0];
            $userinfo[$uid] = $uinfo;
        }
        return $userinfo[$uid];
    }

}
