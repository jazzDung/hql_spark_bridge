create table pf_systemconfig
(
    module_key    varchar(100) not null
        primary key,
    version_value varchar(100),
    module_long   varchar(1000)
);

comment on table pf_systemconfig is '模块版本信息表';

comment on column pf_systemconfig.module_key is '主键KEY';

comment on column pf_systemconfig.version_value is '版本号';

comment on column pf_systemconfig.module_long is '描述';

alter table pf_systemconfig
    owner to postgres;

create table pf_operationlog
(
    id          varchar(50) not null
        primary key,
    oper_user   varchar(50),
    oper_ip     varchar(50),
    oper_date   timestamp,
    oper_model  varchar(150),
    oper_menu   varchar(250),
    oper_method varchar(250),
    oper_type   varchar(50),
    oper_remark varchar(4000)
);

comment on column pf_operationlog.id is 'Primary key ID';

comment on column pf_operationlog.oper_user is 'Operator';

comment on column pf_operationlog.oper_ip is 'Request ip';

comment on column pf_operationlog.oper_date is 'Operation time';

comment on column pf_operationlog.oper_model is 'Operation module';

comment on column pf_operationlog.oper_menu is 'Operation menu';

comment on column pf_operationlog.oper_method is 'Request method';

comment on column pf_operationlog.oper_type is 'Operation type';

comment on column pf_operationlog.oper_remark is 'Operation details';

alter table pf_operationlog
    owner to postgres;

create table pf_menu
(
    id        varchar(50) not null
        primary key,
    name      varchar(60),
    crt_date  varchar(50),
    orders    integer,
    icon      varchar(50),
    leaf_flag varchar(1),
    tip       varchar(255),
    type      varchar(255),
    issamewin varchar(255),
    parent_id varchar(255),
    url       varchar(255),
    sign      varchar(1)
);

comment on table pf_menu is 'menu';

comment on column pf_menu.id is 'Primary key';

comment on column pf_menu.crt_date is 'create date';

comment on column pf_menu.orders is 'sort';

comment on column pf_menu.leaf_flag is 'Leaf node 0 No 1 yes';

comment on column pf_menu.tip is 'tips';

comment on column pf_menu.type is 'Type: module module, page menu';

comment on column pf_menu.issamewin is 'Pop up 0 No 1 yes';

comment on column pf_menu.parent_id is 'parent id';

comment on column pf_menu.sign is 'Main title ID, 0 is not the main title, 1 is the main title';

alter table pf_menu
    owner to postgres;

create table pf_menu_function
(
    f_id          varchar(50) not null
        primary key,
    f_menuid      varchar(50),
    f_functionid  varchar(50),
    f_order       varchar(50),
    function_name varchar(50)
);

comment on table pf_menu_function is 'menu function table';

comment on column pf_menu_function.f_id is 'Primary key';

comment on column pf_menu_function.f_menuid is 'menu id';

comment on column pf_menu_function.f_functionid is 'function id';

comment on column pf_menu_function.f_order is 'sort';

comment on column pf_menu_function.function_name is 'function name';

alter table pf_menu_function
    owner to postgres;

create table pf_user
(
    id                 varchar(50) not null
        primary key,
    user_name          varchar(50),
    name               varchar(50),
    password           varchar(255),
    is_admin           varchar(1),
    flag               varchar(1),
    user_duetime       timestamp,
    employ_duetime     varchar(1),
    remark             varchar(255),
    update_time        timestamp,
    update_user        varchar(50),
    deadline           timestamp,
    failed_login_count integer     not null,
    failed_login_time  timestamp   not null,
    f_common_menu      varchar(255)
);

comment on column pf_user.id is 'Primary key';

comment on column pf_user.user_name is 'user name';

comment on column pf_user.is_admin is 'Super administrator 0 No 1 yes';

comment on column pf_user.flag is 'Status tag 0 enable 1 disable';

comment on column pf_user.user_duetime is 'Term of validity';

comment on column pf_user.employ_duetime is 'Enable validity period (0 is no, 1 is yes)';

comment on column pf_user.update_time is 'update time';

comment on column pf_user.update_user is 'update user';

comment on column pf_user.deadline is 'Password expiration time';

comment on column pf_user.failed_login_count is 'Consecutive login failures';

comment on column pf_user.failed_login_time is 'Login failure time';

comment on column pf_user.f_common_menu is 'User Common menu';

alter table pf_user
    owner to postgres;

create unique index user_name
    on pf_user (user_name);

create index pf_user_name
    on pf_user (name);

create index flag
    on pf_user (flag);

create index pf_user_is_admin
    on pf_user (is_admin);

create table pf_role
(
    role_id     varchar(50) not null
        primary key,
    role_name   varchar(255),
    role_alias  varchar(255),
    create_time timestamp,
    role_desc   varchar(255)
);

comment on column pf_role.role_id is 'role ID';

comment on column pf_role.role_name is 'role name';

comment on column pf_role.role_alias is 'role alias';

comment on column pf_role.create_time is 'create time';

comment on column pf_role.role_desc is 'role desc';

alter table pf_role
    owner to postgres;

create index role_name
    on pf_role (role_name);

create table pf_user_role
(
    id      varchar(50) not null
        primary key,
    user_id varchar(50),
    role_id varchar(50)
);

comment on column pf_user_role.id is 'Primary key';

comment on column pf_user_role.user_id is 'user ID';

comment on column pf_user_role.role_id is 'role ID';

alter table pf_user_role
    owner to postgres;

create table pf_res_attr_data
(
    id           varchar(50) not null
        primary key,
    res_id       varchar(50),
    attr_id      varchar(50),
    res_rec_id   varchar(50),
    attr_rec_id  varchar(50),
    operate_keys varchar(50)
);

comment on column pf_res_attr_data.id is 'Primary key';

comment on column pf_res_attr_data.res_id is 'resource id';

comment on column pf_res_attr_data.attr_id is 'Object type, role obj or user obj';

comment on column pf_res_attr_data.res_rec_id is 'Specific resources: module ID';

comment on column pf_res_attr_data.attr_rec_id is 'Authorization object ID, role ID or user ID';

comment on column pf_res_attr_data.operate_keys is 'Operations on resources, such as adding, deleting and modifying';

alter table pf_res_attr_data
    owner to postgres;

create table pf_data_source
(
    f_datasrcid      varchar(255) not null
        primary key,
    f_datasrcname    varchar(255),
    f_datasrcalias   varchar(255),
    f_datasrcdesc    varchar(255),
    f_user           varchar(255),
    f_password       varchar(255),
    f_drivertype     varchar(255),
    f_driver         varchar(255),
    f_url            varchar(2048),
    f_maxconnum      integer,
    f_dbcharset      varchar(1024),
    f_validation     varchar(255),
    f_isolation      integer,
    f_safeusername   varchar(255),
    f_safefilepath   varchar(255),
    f_failconn       varchar(255),
    f_failconnnumber integer,
    f_failconnsecond integer,
    f_createtime     timestamp,
    f_maxwaitnum     integer
);

comment on column pf_data_source.f_datasrcid is 'Primary key';

comment on column pf_data_source.f_datasrcname is 'data name';

comment on column pf_data_source.f_datasrcalias is 'data alias';

comment on column pf_data_source.f_datasrcdesc is 'description';

comment on column pf_data_source.f_user is 'username';

comment on column pf_data_source.f_password is 'password';

comment on column pf_data_source.f_drivertype is 'drive type';

comment on column pf_data_source.f_driver is 'JDBC driver';

comment on column pf_data_source.f_url is 'Connection address';

comment on column pf_data_source.f_maxconnum is 'maximum number of connections';

comment on column pf_data_source.f_dbcharset is 'character set';

comment on column pf_data_source.f_validation is 'Check SQL';

comment on column pf_data_source.f_isolation is 'transaction isolation level';

comment on column pf_data_source.f_safeusername is 'key user';

comment on column pf_data_source.f_safefilepath is 'key file';

comment on column pf_data_source.f_failconn is 'failed reconnection';

comment on column pf_data_source.f_failconnnumber is 'number of failed reconnections';

comment on column pf_data_source.f_failconnsecond is 'failed reconnection interval';

comment on column pf_data_source.f_createtime is 'create time';

comment on column pf_data_source.f_maxwaitnum is 'Maximum waiting milliseconds';

alter table pf_data_source
    owner to postgres;

create index f_datasrcalias
    on pf_data_source (f_datasrcalias);

create table jcm_etlserver
(
    f_id               varchar(100) not null
        primary key,
    f_etlname          varchar(255) not null,
    f_alias            varchar(255),
    f_etltype          varchar(255) not null,
    f_etlip            varchar(255),
    f_username         varchar(500),
    f_password         varchar(255),
    f_aptname          varchar(500),
    f_etlmemo          varchar(255),
    f_port             varchar(255),
    f_area             varchar(2000),
    f_intergratservice varchar(255),
    f_idf              varchar(255),
    f_isuseusrpwd      varchar(255),
    f_bpschesvcid      varchar(255),
    f_bpcallsvcscnid   varchar(255),
    f_calltype         varchar(255),
    f_bpscheqrysvcid   varchar(255),
    f_bpqrysvcscnid    varchar(255),
    f_querytype        varchar(255),
    f_queryfailretcode varchar(255),
    f_runningcode      varchar(255)
);

comment on table jcm_etlserver is 'ETL server';

comment on column jcm_etlserver.f_id is 'Primary key';

comment on column jcm_etlserver.f_etlname is 'etl name';

comment on column jcm_etlserver.f_alias is 'alias';

comment on column jcm_etlserver.f_etltype is 'type';

comment on column jcm_etlserver.f_etlip is 'ip address';

comment on column jcm_etlserver.f_username is 'username';

comment on column jcm_etlserver.f_password is 'password';

comment on column jcm_etlserver.f_aptname is 'Profile name';

comment on column jcm_etlserver.f_etlmemo is 'describe';

comment on column jcm_etlserver.f_port is 'port';

comment on column jcm_etlserver.f_area is 'area';

comment on column jcm_etlserver.f_intergratservice is 'Integrated services';

comment on column jcm_etlserver.f_idf is 'INFA_DOMAINS_FILE';

comment on column jcm_etlserver.f_isuseusrpwd is 'Whether to transfer login information';

comment on column jcm_etlserver.f_bpschesvcid is 'batch job scheduling service ID';

comment on column jcm_etlserver.f_bpcallsvcscnid is 'batch job initiation service scenario ID';

comment on column jcm_etlserver.f_calltype is 'call up return code';

comment on column jcm_etlserver.f_bpscheqrysvcid is 'batch job scheduling query service ID';

comment on column jcm_etlserver.f_bpqrysvcscnid is 'batch job query service scenario ID';

comment on column jcm_etlserver.f_querytype is 'query return code';

comment on column jcm_etlserver.f_queryfailretcode is 'query failure return code';

comment on column jcm_etlserver.f_runningcode is 'runningcode';

alter table jcm_etlserver
    owner to postgres;

create index f_alias
    on jcm_etlserver (f_alias);

create table jcm_proxyserver
(
    f_id                   varchar(50) not null
        primary key,
    f_proxyname            varchar(50),
    f_alias                varchar(200),
    f_proxyip              varchar(20),
    f_proxyport            varchar(10),
    f_concurrentjob        varchar(10),
    f_logsavetime          varchar(10),
    f_recordsavetime       varchar(10),
    f_cpuusage             varchar(10),
    f_physicalramusge      varchar(10),
    f_username             varchar(50),
    f_password             varchar(50),
    f_proxymemo            varchar(500),
    f_memory_utilization   varchar(10),
    f_cpu_utilization      varchar(10),
    f_appname              varchar(50),
    f_proxy_group_id       varchar(255),
    f_jvm_utilization      varchar(255),
    f_hardware_utilization varchar(255),
    f_cluster_ip           varchar(255),
    f_startfilescan        varchar(255),
    f_deploy_user          varchar(255),
    f_deploy_path          varchar(255),
    f_deploy_password      varchar(255),
    f_deploy_sshkey        varchar(255),
    f_totalresource        varchar(255),
    f_enable               varchar(255)
);

comment on table jcm_proxyserver is 'proxy server table';

comment on column jcm_proxyserver.f_id is 'Primary key';

comment on column jcm_proxyserver.f_proxyname is 'name';

comment on column jcm_proxyserver.f_alias is 'alias';

comment on column jcm_proxyserver.f_proxyip is 'ip address';

comment on column jcm_proxyserver.f_proxyport is 'port';

comment on column jcm_proxyserver.f_concurrentjob is 'concurrentvjob';

comment on column jcm_proxyserver.f_logsavetime is 'log savetime';

comment on column jcm_proxyserver.f_recordsavetime is 'record savetime';

comment on column jcm_proxyserver.f_cpuusage is 'cpu usage';

comment on column jcm_proxyserver.f_physicalramusge is 'physical ramusge';

comment on column jcm_proxyserver.f_username is 'user name';

comment on column jcm_proxyserver.f_password is 'password';

comment on column jcm_proxyserver.f_proxymemo is 'memo';

comment on column jcm_proxyserver.f_memory_utilization is 'memory utilization';

comment on column jcm_proxyserver.f_cpu_utilization is 'cpu utilization';

comment on column jcm_proxyserver.f_appname is 'app name';

comment on column jcm_proxyserver.f_proxy_group_id is 'proxy group id';

comment on column jcm_proxyserver.f_jvm_utilization is 'jvm utilization';

comment on column jcm_proxyserver.f_hardware_utilization is 'hardware utilization';

comment on column jcm_proxyserver.f_cluster_ip is 'cluster ip';

comment on column jcm_proxyserver.f_startfilescan is 'Whether to enable file scanning';

comment on column jcm_proxyserver.f_deploy_user is 'DEPLOY USER';

comment on column jcm_proxyserver.f_deploy_path is 'DEPLOY PATH';

comment on column jcm_proxyserver.f_deploy_password is 'DEPLOY PASSWORD';

comment on column jcm_proxyserver.f_deploy_sshkey is 'DEPLOY SSHKEY';

comment on column jcm_proxyserver.f_totalresource is 'The total resource';

comment on column jcm_proxyserver.f_enable is 'Status 1: Enable; 0: stop using';

alter table jcm_proxyserver
    owner to postgres;

create table jcm_proxygroup
(
    f_id    varchar(50) not null
        primary key,
    f_name  varchar(50),
    f_alias varchar(200),
    f_desc  varchar(500)
);

comment on table jcm_proxygroup is 'proxy group table';

comment on column jcm_proxygroup.f_id is 'Primary key';

comment on column jcm_proxygroup.f_name is 'name';

comment on column jcm_proxygroup.f_alias is 'alias';

comment on column jcm_proxygroup.f_desc is 'desc';

alter table jcm_proxygroup
    owner to postgres;

create table jcm_globalapp
(
    f_id       varchar(50) not null
        primary key,
    f_syscode  varchar(50) not null,
    f_name     varchar(200),
    f_status   varchar(200),
    f_order    integer     not null,
    f_desc     varchar(500),
    f_flopid   varchar(255),
    f_flowid   varchar(255),
    f_flopname varchar(255),
    f_flowname varchar(255)
);

comment on table jcm_globalapp is 'Application system configuration table';

comment on column jcm_globalapp.f_id is 'Primary key';

comment on column jcm_globalapp.f_syscode is 'system code';

comment on column jcm_globalapp.f_name is 'system name';

comment on column jcm_globalapp.f_status is 'Status tag 0 enable 1 disable';

comment on column jcm_globalapp.f_order is 'sort';

comment on column jcm_globalapp.f_desc is 'describe';

comment on column jcm_globalapp.f_flopid is 'flop id';

comment on column jcm_globalapp.f_flowid is 'flow id';

comment on column jcm_globalapp.f_flopname is 'flop name';

comment on column jcm_globalapp.f_flowname is 'flow name';

alter table jcm_globalapp
    owner to postgres;

create table pf_role_resource
(
    id           varchar(50) not null
        primary key,
    roleid       varchar(50),
    restype      varchar(50),
    resid        varchar(50),
    operate_keys varchar(200)
);

comment on table pf_role_resource is 'Role permission resource table';

comment on column pf_role_resource.id is 'Primary key';

comment on column pf_role_resource.roleid is 'role ID';

comment on column pf_role_resource.restype is 'resource type';

comment on column pf_role_resource.resid is 'resource id';

comment on column pf_role_resource.operate_keys is 'Resource operation permission';

alter table pf_role_resource
    owner to postgres;

create table jcm_globalapp_type
(
    f_id        varchar(50) not null
        primary key,
    f_syscode   varchar(50) not null,
    f_dickey    varchar(1200),
    f_dicvalue  varchar(100),
    f_parentkey varchar(255)
);

comment on table jcm_globalapp_type is 'Business classification table';

comment on column jcm_globalapp_type.f_id is 'Primary key';

comment on column jcm_globalapp_type.f_syscode is 'system code';

comment on column jcm_globalapp_type.f_dickey is 'Business classification key';

comment on column jcm_globalapp_type.f_dicvalue is 'Business classification value';

comment on column jcm_globalapp_type.f_parentkey is 'Business parent key';

alter table jcm_globalapp_type
    owner to postgres;

create table jcm_filescan
(
    f_id         varchar(50) not null
        primary key,
    f_proxyid    varchar(50) not null,
    f_basedir    varchar(200),
    f_filesuffix varchar(20),
    f_isasc      varchar(10),
    f_listenerid varchar(20),
    f_period     varchar(10),
    f_flop       varchar(10),
    f_flopsql    varchar(1000),
    f_flopdsid   varchar(100),
    f_scantype   varchar(200),
    f_starttime  timestamp,
    f_endtime    timestamp,
    f_systems    varchar(4000),
    f_schename   varchar(50)
);

comment on table jcm_filescan is 'File scan configuration table';

comment on column jcm_filescan.f_id is 'Primary key';

comment on column jcm_filescan.f_proxyid is 'Proxy Server ID';

comment on column jcm_filescan.f_basedir is 'Listen on root';

comment on column jcm_filescan.f_filesuffix is 'Listens for files in the specified format';

comment on column jcm_filescan.f_isasc is 'date sort';

comment on column jcm_filescan.f_listenerid is 'Listening for server identity';

comment on column jcm_filescan.f_period is 'Time between scans (seconds)';

comment on column jcm_filescan.f_flop is 'Whether flop';

comment on column jcm_filescan.f_flopsql is 'Flip the SQL';

comment on column jcm_filescan.f_flopdsid is 'Flipping data source';

comment on column jcm_filescan.f_scantype is 'File scan type';

comment on column jcm_filescan.f_starttime is 'Scanning start time';

comment on column jcm_filescan.f_endtime is 'Scanning End Time';

comment on column jcm_filescan.f_systems is 'List of systems to be processed';

comment on column jcm_filescan.f_schename is 'Scheduling server';

alter table jcm_filescan
    owner to postgres;

create table jcm_globalflip
(
    f_syscode       varchar(50) not null
        primary key,
    f_previousodate varchar(12) not null,
    f_flipodate     varchar(12),
    f_status        varchar(1),
    f_updatedate    timestamp
);

comment on table jcm_globalflip is 'Application System Flip List';

comment on column jcm_globalflip.f_syscode is 'Primary key (application code)';

comment on column jcm_globalflip.f_previousodate is 'Last execution date';

comment on column jcm_globalflip.f_flipodate is 'Flip the date';

comment on column jcm_globalflip.f_status is 'Double card status';

comment on column jcm_globalflip.f_updatedate is 'Flip date update time';

alter table jcm_globalflip
    owner to postgres;

create table jcm_warningconfig
(
    f_id         varchar(50) not null
        primary key,
    f_name       varchar(255),
    f_alias      varchar(255),
    f_desc       varchar(1000),
    f_type       varchar(255),
    f_enable     varchar(255),
    f_alarmtime  varchar(255),
    f_offset     integer,
    f_createtime timestamp,
    f_updatetime timestamp
);

comment on table jcm_warningconfig is 'Warning config';

comment on column jcm_warningconfig.f_id is 'primary key';

comment on column jcm_warningconfig.f_name is 'Warning name';

comment on column jcm_warningconfig.f_alias is 'Warning alias';

comment on column jcm_warningconfig.f_desc is 'Warning desc';

comment on column jcm_warningconfig.f_type is 'Warning type';

comment on column jcm_warningconfig.f_enable is 'enable';

comment on column jcm_warningconfig.f_alarmtime is 'AlarmTime';

comment on column jcm_warningconfig.f_offset is 'Offset';

comment on column jcm_warningconfig.f_createtime is 'create time';

comment on column jcm_warningconfig.f_updatetime is 'update time';

alter table jcm_warningconfig
    owner to postgres;

create table jcm_warning_flow
(
    f_id        varchar(50) not null
        primary key,
    f_warningid varchar(50),
    f_flowid    varchar(255),
    f_flowname  varchar(255)
);

comment on table jcm_warning_flow is 'Warning flow relation';

comment on column jcm_warning_flow.f_id is 'primary key';

comment on column jcm_warning_flow.f_warningid is 'Warning id';

comment on column jcm_warning_flow.f_flowid is 'flow id';

comment on column jcm_warning_flow.f_flowname is 'flow name';

alter table jcm_warning_flow
    owner to postgres;

create table jcm_warning_record
(
    f_id            varchar(50) not null
        primary key,
    f_messageid     varchar(255),
    f_warningid     varchar(255),
    f_flowid        varchar(255),
    f_lastalarmtime timestamp
);

comment on table jcm_warning_record is 'Warning  record';

comment on column jcm_warning_record.f_id is 'primary key';

comment on column jcm_warning_record.f_messageid is 'message configuration ID';

comment on column jcm_warning_record.f_warningid is 'Warning id';

comment on column jcm_warning_record.f_flowid is 'flow id';

comment on column jcm_warning_record.f_lastalarmtime is 'Last alarmTime';

alter table jcm_warning_record
    owner to postgres;

create table jcm_warning_log
(
    f_id          varchar(50) not null
        primary key,
    f_warningname varchar(255),
    f_warningtype varchar(255),
    f_warningmode varchar(255),
    f_read        varchar(255),
    f_createtime  timestamp,
    f_log         varchar(4000)
);

comment on table jcm_warning_log is 'Warning  log';

comment on column jcm_warning_log.f_id is 'primary key';

comment on column jcm_warning_log.f_warningname is 'Warning name';

comment on column jcm_warning_log.f_warningtype is 'Warning type';

comment on column jcm_warning_log.f_warningmode is 'Warning mode';

comment on column jcm_warning_log.f_read is 'read flag';

comment on column jcm_warning_log.f_createtime is 'create time';

comment on column jcm_warning_log.f_log is 'Warning log';

alter table jcm_warning_log
    owner to postgres;

create table jcm_server
(
    f_id              varchar(50) not null
        primary key,
    f_type            varchar(50),
    f_name            varchar(50),
    f_alias           varchar(200),
    f_ip              varchar(20),
    f_port            varchar(10),
    f_appname         varchar(50),
    f_protocol        varchar(20),
    f_memo            varchar(500),
    f_cluster_ip      varchar(255),
    f_deploy_user     varchar(255),
    f_deploy_path     varchar(255),
    f_deploy_password varchar(255),
    f_deploy_sshkey   varchar(50)
);

comment on table jcm_server is 'server table';

comment on column jcm_server.f_id is 'Primary key';

comment on column jcm_server.f_type is 'Server type';

comment on column jcm_server.f_name is 'Server name';

comment on column jcm_server.f_alias is 'Server alias';

comment on column jcm_server.f_ip is 'Server ip';

comment on column jcm_server.f_port is 'Server port';

comment on column jcm_server.f_appname is 'Server appName';

comment on column jcm_server.f_protocol is 'Server protocol';

comment on column jcm_server.f_memo is 'Server demo';

comment on column jcm_server.f_cluster_ip is 'cluster ip';

comment on column jcm_server.f_deploy_user is 'DEPLOY USER';

comment on column jcm_server.f_deploy_path is 'DEPLOY PATH';

comment on column jcm_server.f_deploy_password is 'DEPLOY PASSWORD';

comment on column jcm_server.f_deploy_sshkey is 'DEPLOY SSHKEY';

alter table jcm_server
    owner to postgres;

create table jcm_emailserver
(
    f_id            varchar(50) not null
        primary key,
    f_emailname     varchar(200),
    f_emailalias    varchar(200),
    f_emailaddress  varchar(100),
    f_emilusername  varchar(100),
    f_emailpassword varchar(50),
    f_emailport     varchar(100),
    f_emailmemo     varchar(500)
);

comment on table jcm_emailserver is 'Mail server table';

comment on column jcm_emailserver.f_id is 'Primary key';

comment on column jcm_emailserver.f_emailname is 'Mail server name';

comment on column jcm_emailserver.f_emailalias is 'Mail server alias';

comment on column jcm_emailserver.f_emailaddress is 'Mail server address';

comment on column jcm_emailserver.f_emilusername is 'user';

comment on column jcm_emailserver.f_emailpassword is 'password';

comment on column jcm_emailserver.f_emailport is 'Sender mailbox';

comment on column jcm_emailserver.f_emailmemo is 'memo';

alter table jcm_emailserver
    owner to postgres;

create table jcm_message
(
    f_id         varchar(50) not null
        primary key,
    f_name       varchar(200),
    f_alias      varchar(200),
    f_type       varchar(50),
    f_title      varchar(500),
    f_body       varchar(500),
    f_issendlog  varchar(10),
    f_sendlimits varchar(50),
    f_rangtype   varchar(50),
    f_desc       varchar(500)
);

comment on table jcm_message is 'Message table';

comment on column jcm_message.f_id is 'Primary key';

comment on column jcm_message.f_name is 'name';

comment on column jcm_message.f_alias is 'alias';

comment on column jcm_message.f_type is 'type';

comment on column jcm_message.f_title is 'title';

comment on column jcm_message.f_body is 'body';

comment on column jcm_message.f_issendlog is 'Whether to send logs';

comment on column jcm_message.f_sendlimits is 'Send range';

comment on column jcm_message.f_rangtype is 'Sphere of influence';

comment on column jcm_message.f_desc is 'desc';

alter table jcm_message
    owner to postgres;

create table jcm_contactconfig
(
    f_id          varchar(50)  not null
        primary key,
    f_contactname varchar(100) not null,
    f_syscode     varchar(400),
    f_email       varchar(50),
    f_mobile      varchar(50),
    f_desc        varchar(500)
);

comment on table jcm_contactconfig is 'Contact config table';

comment on column jcm_contactconfig.f_id is 'Primary key';

comment on column jcm_contactconfig.f_contactname is 'Name';

comment on column jcm_contactconfig.f_syscode is 'Syscode';

comment on column jcm_contactconfig.f_email is 'Email';

comment on column jcm_contactconfig.f_mobile is 'Mobile';

comment on column jcm_contactconfig.f_desc is 'Desc';

alter table jcm_contactconfig
    owner to postgres;

create table jcm_message_contact
(
    f_id        varchar(50) not null
        primary key,
    f_messageid varchar(50) not null,
    f_contactid varchar(50) not null
);

comment on table jcm_message_contact is 'Message Contact table';

comment on column jcm_message_contact.f_id is 'Primary key';

comment on column jcm_message_contact.f_messageid is 'message id';

comment on column jcm_message_contact.f_contactid is 'contact id';

alter table jcm_message_contact
    owner to postgres;

create table jcm_resourcerels
(
    f_id         varchar(50) not null
        primary key,
    f_apptype    varchar(50),
    f_sourceid   varchar(50),
    f_sourcetype varchar(50),
    f_targetid   varchar(50),
    f_targettype varchar(50)
);

comment on table jcm_resourcerels is 'Messaging configuration table';

comment on column jcm_resourcerels.f_id is 'Primary key';

comment on column jcm_resourcerels.f_apptype is 'Resource type';

comment on column jcm_resourcerels.f_sourceid is 'Message ID';

comment on column jcm_resourcerels.f_sourcetype is 'Message Type';

comment on column jcm_resourcerels.f_targetid is 'Target resource ID';

comment on column jcm_resourcerels.f_targettype is 'Target resource type';

alter table jcm_resourcerels
    owner to postgres;

create table jcm_system_param
(
    f_id         varchar(50) not null
        primary key,
    f_paramname  varchar(100),
    f_paramvalue varchar(1000),
    f_paramorder integer,
    f_paramdes   varchar(500)
);

comment on table jcm_system_param is 'System param table';

comment on column jcm_system_param.f_id is 'Primary key';

comment on column jcm_system_param.f_paramname is 'System param name';

comment on column jcm_system_param.f_paramvalue is 'System param value';

comment on column jcm_system_param.f_paramorder is 'System param order';

comment on column jcm_system_param.f_paramdes is 'System param desc';

alter table jcm_system_param
    owner to postgres;

create table jcm_parameter
(
    f_id             varchar(100) not null
        primary key,
    f_paramtype      varchar(255) not null,
    f_paramname      varchar(255) not null,
    f_alias          varchar(255),
    f_parammemo      varchar(255),
    f_paramvarname   varchar(255),
    f_paramvaluetype varchar(255),
    f_paramvalue     varchar(1000),
    f_datasourceid   varchar(255),
    f_syscode        varchar(100),
    f_dickey         varchar(1200)
);

comment on table jcm_parameter is 'parameter table';

comment on column jcm_parameter.f_id is 'Primary key';

comment on column jcm_parameter.f_paramtype is 'param type';

comment on column jcm_parameter.f_paramname is 'param name';

comment on column jcm_parameter.f_alias is 'alias';

comment on column jcm_parameter.f_parammemo is 'desc';

comment on column jcm_parameter.f_paramvarname is 'param varname';

comment on column jcm_parameter.f_paramvaluetype is 'param value type';

comment on column jcm_parameter.f_paramvalue is 'param value';

comment on column jcm_parameter.f_datasourceid is 'datasource id';

comment on column jcm_parameter.f_syscode is 'Application system code';

comment on column jcm_parameter.f_dickey is 'Business classification key';

alter table jcm_parameter
    owner to postgres;

create table jcm_job
(
    f_id             varchar(100) not null
        primary key,
    f_name           varchar(255),
    f_alias          varchar(255),
    f_desc           varchar(255),
    f_type           varchar(255),
    f_program        varchar(255),
    f_path           varchar(255),
    f_jobserver      varchar(255),
    f_project        varchar(255),
    f_datasource     varchar(255),
    f_jobagent       varchar(255),
    f_source         varchar(255),
    f_target         varchar(255),
    f_exitcode       varchar(255),
    f_begindate      date,
    f_enddate        date,
    f_concurrency    varchar(255),
    f_version        varchar(255),
    f_runstrategy    varchar(255),
    f_syscode        varchar(255),
    f_rerun          varchar(255),
    f_forbidden      varchar(255),
    f_priority       integer,
    f_debug          varchar(255),
    f_msgtype        varchar(255),
    f_dickey         varchar(1200),
    f_createtime     timestamp,
    f_updatetime     timestamp,
    f_jobresource    varchar(255),
    f_jobcurrencyid  varchar(255),
    f_agencyfailover varchar(255)
);

comment on table jcm_job is 'job table';

comment on column jcm_job.f_id is 'Primary key';

comment on column jcm_job.f_name is 'job name';

comment on column jcm_job.f_alias is 'job alias';

comment on column jcm_job.f_desc is 'job desc';

comment on column jcm_job.f_type is 'job type';

comment on column jcm_job.f_program is 'job programname';

comment on column jcm_job.f_path is 'job programpath';

comment on column jcm_job.f_jobserver is 'job server';

comment on column jcm_job.f_project is 'job projectname';

comment on column jcm_job.f_datasource is 'job datasource';

comment on column jcm_job.f_jobagent is 'job agent';

comment on column jcm_job.f_source is 'source system';

comment on column jcm_job.f_target is 'target system';

comment on column jcm_job.f_exitcode is 'exit code';

comment on column jcm_job.f_begindate is 'begin date';

comment on column jcm_job.f_enddate is 'end date';

comment on column jcm_job.f_concurrency is 'concurrency';

comment on column jcm_job.f_version is 'version';

comment on column jcm_job.f_runstrategy is 'runstrategy';

comment on column jcm_job.f_syscode is 'Application system code';

comment on column jcm_job.f_rerun is 'rerun';

comment on column jcm_job.f_forbidden is 'forbidden';

comment on column jcm_job.f_priority is 'priority';

comment on column jcm_job.f_debug is 'debug';

comment on column jcm_job.f_msgtype is 'msgtype';

comment on column jcm_job.f_dickey is 'Business classification key';

comment on column jcm_job.f_createtime is 'create time';

comment on column jcm_job.f_updatetime is 'update time';

comment on column jcm_job.f_jobresource is 'Job resource';

comment on column jcm_job.f_jobcurrencyid is 'Job global concurrency limit ID';

comment on column jcm_job.f_agencyfailover is 'agent failover';

alter table jcm_job
    owner to postgres;

create table jcm_param
(
    f_id      varchar(100) not null
        primary key,
    f_jobid   varchar(255) not null,
    f_paramid varchar(255) not null,
    f_seq     integer,
    f_type    varchar(255),
    f_manual  varchar(255),
    f_format  varchar(255)
);

comment on table jcm_param is 'job table';

comment on column jcm_param.f_id is 'Primary key';

comment on column jcm_param.f_jobid is 'job id';

comment on column jcm_param.f_paramid is 'parameter id';

comment on column jcm_param.f_seq is 'param seq';

comment on column jcm_param.f_type is 'param type';

comment on column jcm_param.f_manual is 'param manual';

comment on column jcm_param.f_format is 'param format';

alter table jcm_param
    owner to postgres;

create table jcm_jobfrequency
(
    f_id       varchar(100) not null
        primary key,
    f_name     varchar(255) not null,
    f_alias    varchar(255),
    f_desc     varchar(255),
    f_execute  varchar(255),
    f_type     varchar(255),
    f_months   varchar(255),
    f_days     varchar(255),
    f_weeks    varchar(255),
    f_weekdays varchar(255)
);

comment on table jcm_jobfrequency is 'jobfrequency table';

comment on column jcm_jobfrequency.f_id is 'Primary key';

comment on column jcm_jobfrequency.f_name is 'jobfrequency name';

comment on column jcm_jobfrequency.f_alias is 'jobfrequency alias';

comment on column jcm_jobfrequency.f_desc is 'jobfrequency desc';

comment on column jcm_jobfrequency.f_execute is 'jobfrequency execute';

comment on column jcm_jobfrequency.f_type is 'jobfrequency type';

comment on column jcm_jobfrequency.f_months is 'jobfrequency months';

comment on column jcm_jobfrequency.f_days is 'jobfrequency days';

comment on column jcm_jobfrequency.f_weeks is 'jobfrequency weeks';

comment on column jcm_jobfrequency.f_weekdays is 'jobfrequency weekdays';

alter table jcm_jobfrequency
    owner to postgres;

create table jcm_jobfrequencyrelation
(
    f_id             varchar(100) not null
        primary key,
    f_jobid          varchar(255) not null,
    f_jobfrequencyid varchar(255) not null
);

comment on table jcm_jobfrequencyrelation is 'jobfrequency relation';

comment on column jcm_jobfrequencyrelation.f_id is 'Primary key';

comment on column jcm_jobfrequencyrelation.f_jobid is 'job id';

comment on column jcm_jobfrequencyrelation.f_jobfrequencyid is 'jobfrequency id';

alter table jcm_jobfrequencyrelation
    owner to postgres;

create table jcm_flow
(
    f_id              varchar(50)  not null
        primary key,
    f_name            varchar(255) not null,
    f_alias           varchar(255),
    f_syscode         varchar(100),
    f_schedserver     varchar(100),
    f_retry           varchar(20),
    f_pattern         varchar(10),
    f_retryinterval   integer,
    f_retrymaxnum     integer,
    f_checkstatus     varchar(10),
    f_runstrategy     varchar(10),
    f_desc            varchar(500),
    f_effectivedate   timestamp,
    f_finisheddate    timestamp,
    f_priority        integer,
    f_dickey          varchar(1200),
    f_createtime      timestamp,
    f_updatetime      timestamp,
    f_numberoflinks   integer,
    f_criticalpath    varchar(255),
    f_providedataflow varchar(255)
);

comment on table jcm_flow is 'Job flow definition table';

comment on column jcm_flow.f_id is 'Primary key';

comment on column jcm_flow.f_name is 'jobfrequency name';

comment on column jcm_flow.f_alias is 'jobfrequency alias';

comment on column jcm_flow.f_syscode is 'Application system code';

comment on column jcm_flow.f_schedserver is 'Dispatch server';

comment on column jcm_flow.f_retry is 'Error handling';

comment on column jcm_flow.f_pattern is 'Retry mode';

comment on column jcm_flow.f_retryinterval is 'Retry interval';

comment on column jcm_flow.f_retrymaxnum is 'Maximum number of retries';

comment on column jcm_flow.f_checkstatus is 'Check the last execution status';

comment on column jcm_flow.f_runstrategy is 'Under the same data date, only one successful run is allowed';

comment on column jcm_flow.f_desc is 'jobfrequency desc';

comment on column jcm_flow.f_effectivedate is 'Effective time';

comment on column jcm_flow.f_finisheddate is 'stop the time';

comment on column jcm_flow.f_priority is 'priority';

comment on column jcm_flow.f_dickey is 'Business classification key';

comment on column jcm_flow.f_createtime is 'create time';

comment on column jcm_flow.f_updatetime is 'update time';

comment on column jcm_flow.f_numberoflinks is 'Number of links';

comment on column jcm_flow.f_criticalpath is 'The critical path';

comment on column jcm_flow.f_providedataflow is 'The feed job stream';

alter table jcm_flow
    owner to postgres;

create table jcm_flowrelation
(
    f_id         varchar(50) not null
        primary key,
    f_jobflowid  varchar(50) not null,
    f_jobid      varchar(50) not null,
    f_sourcetype varchar(20),
    f_parent     varchar(50),
    f_parenttype varchar(20),
    f_xpos       integer,
    f_ypos       integer,
    f_width      integer,
    f_height     integer,
    f_custom     varchar(100),
    f_propsex    varchar(100)
);

comment on table jcm_flowrelation is 'Job flow dependency table';

comment on column jcm_flowrelation.f_id is 'Primary key';

comment on column jcm_flowrelation.f_jobflowid is 'Job Flow Id';

comment on column jcm_flowrelation.f_jobid is 'job id';

comment on column jcm_flowrelation.f_sourcetype is 'Node type';

comment on column jcm_flowrelation.f_parent is 'Parent Id';

comment on column jcm_flowrelation.f_parenttype is 'Parent type';

comment on column jcm_flowrelation.f_xpos is 'X coordinate';

comment on column jcm_flowrelation.f_ypos is 'Y coordinate';

comment on column jcm_flowrelation.f_width is 'Width';

comment on column jcm_flowrelation.f_height is 'Height';

comment on column jcm_flowrelation.f_custom is 'Extension field 1';

comment on column jcm_flowrelation.f_propsex is 'Extension field 2';

alter table jcm_flowrelation
    owner to postgres;

create table jcm_xjobpreprocess
(
    f_id        varchar(50) not null
        primary key,
    f_xjobid    varchar(50) not null,
    f_prexjobid varchar(50) not null,
    f_type      varchar(20) not null,
    f_sign      varchar(50),
    f_condtype  varchar(50),
    f_value     varchar(100),
    f_odate     varchar(20)
);

comment on table jcm_xjobpreprocess is 'Workflow front table';

comment on column jcm_xjobpreprocess.f_id is 'Primary key';

comment on column jcm_xjobpreprocess.f_xjobid is 'Job Flow Id';

comment on column jcm_xjobpreprocess.f_prexjobid is 'Predecessor ID';

comment on column jcm_xjobpreprocess.f_type is 'Predecessor type';

comment on column jcm_xjobpreprocess.f_sign is 'Predecessor completion indicator';

comment on column jcm_xjobpreprocess.f_condtype is 'Predecessor status';

comment on column jcm_xjobpreprocess.f_value is 'Triggering conditions';

comment on column jcm_xjobpreprocess.f_odate is 'odate';

alter table jcm_xjobpreprocess
    owner to postgres;

create index jcm_xjobpreprocess_id_index
    on jcm_xjobpreprocess (f_xjobid, f_prexjobid);

create table jcm_jobbranch
(
    f_id          varchar(50) not null
        primary key,
    f_jobflowid   varchar(50),
    f_code        varchar(50),
    f_jobid       varchar(50),
    f_targetid    varchar(50),
    f_type        varchar(20),
    f_typeboolean varchar(100),
    f_paravarname varchar(50),
    f_sourcetype  varchar(50)
);

comment on table jcm_jobbranch is 'Workflow branch condition table';

comment on column jcm_jobbranch.f_id is 'Primary key';

comment on column jcm_jobbranch.f_jobflowid is 'Job Flow Id';

comment on column jcm_jobbranch.f_jobid is 'Node id';

comment on column jcm_jobbranch.f_targetid is 'Target node ID';

comment on column jcm_jobbranch.f_type is 'Branch condition type';

comment on column jcm_jobbranch.f_typeboolean is 'Boolean expression';

comment on column jcm_jobbranch.f_paravarname is 'parameter name';

comment on column jcm_jobbranch.f_sourcetype is 'Node type';

alter table jcm_jobbranch
    owner to postgres;

create table jcm_jobconcurrency
(
    f_id             varchar(50)  not null
        primary key,
    f_name           varchar(255) not null,
    f_alias          varchar(255),
    f_proxycontrol   varchar(1),
    f_proxycurrency  integer,
    f_globalcontrol  varchar(1),
    f_globalcurrency integer,
    f_desc           varchar(1000)
);

comment on table jcm_jobconcurrency is 'Global job concurrency control table';

comment on column jcm_jobconcurrency.f_id is 'Primary key';

comment on column jcm_jobconcurrency.f_name is 'name';

comment on column jcm_jobconcurrency.f_alias is 'alias';

comment on column jcm_jobconcurrency.f_proxycontrol is 'Proxy node concurrency limits';

comment on column jcm_jobconcurrency.f_globalcontrol is 'Global concurrency limit';

comment on column jcm_jobconcurrency.f_desc is 'desc';

alter table jcm_jobconcurrency
    owner to postgres;

create table jcm_jobjumpqueue
(
    f_id     varchar(50) not null
        primary key,
    f_flowid varchar(50) not null,
    f_jobid  varchar(50) not null
);

comment on table jcm_jobjumpqueue is 'Job queue jumping execution table';

comment on column jcm_jobjumpqueue.f_id is 'Primary key';

comment on column jcm_jobjumpqueue.f_flowid is 'Job Flow Id';

comment on column jcm_jobjumpqueue.f_jobid is 'job id';

alter table jcm_jobjumpqueue
    owner to postgres;

create table jcm_calendar
(
    f_id       varchar(100)  not null
        primary key,
    f_name     varchar(255)  not null,
    f_alias    varchar(255),
    f_desc     varchar(255),
    f_year     integer       not null,
    f_datelist varchar(4000) not null
);

comment on table jcm_calendar is 'calendar table';

comment on column jcm_calendar.f_id is 'Primary key';

comment on column jcm_calendar.f_name is 'name';

comment on column jcm_calendar.f_alias is 'alias';

comment on column jcm_calendar.f_desc is 'desc';

comment on column jcm_calendar.f_year is 'year';

comment on column jcm_calendar.f_datelist is 'datelist';

alter table jcm_calendar
    owner to postgres;

create table jcm_frequency
(
    f_id         varchar(100) not null
        primary key,
    f_name       varchar(255) not null,
    f_alias      varchar(255),
    f_type       varchar(255) not null,
    f_starttime  timestamp    not null,
    f_expression varchar(255) not null
);

comment on table jcm_frequency is 'frequency table';

comment on column jcm_frequency.f_id is 'Primary key';

comment on column jcm_frequency.f_name is 'name';

comment on column jcm_frequency.f_alias is 'alias';

comment on column jcm_frequency.f_type is 'type';

comment on column jcm_frequency.f_starttime is 'starttime';

comment on column jcm_frequency.f_expression is 'expression';

alter table jcm_frequency
    owner to postgres;

create table jcm_schedule
(
    f_id           varchar(100) not null
        primary key,
    f_name         varchar(255),
    f_alias        varchar(255),
    f_desc         varchar(255),
    f_starttime    timestamp,
    f_lltime       timestamp,
    f_ultime       timestamp,
    f_calendarid   varchar(255),
    f_frequencyid  varchar(255),
    f_messageid    varchar(255),
    f_effective    varchar(255),
    f_dateformatid varchar(255),
    f_syscode      varchar(255),
    f_createtime   timestamp,
    f_updatetime   timestamp,
    f_dickey       varchar(1200)
);

comment on table jcm_schedule is 'schedule table';

comment on column jcm_schedule.f_id is 'Primary key';

comment on column jcm_schedule.f_name is 'name';

comment on column jcm_schedule.f_alias is 'alias';

comment on column jcm_schedule.f_desc is 'desc';

comment on column jcm_schedule.f_starttime is 'starttime';

comment on column jcm_schedule.f_lltime is 'lltime';

comment on column jcm_schedule.f_ultime is 'ultime';

comment on column jcm_schedule.f_calendarid is 'calendar id';

comment on column jcm_schedule.f_frequencyid is 'frequency id';

comment on column jcm_schedule.f_messageid is 'message id';

comment on column jcm_schedule.f_effective is 'effective';

comment on column jcm_schedule.f_dateformatid is 'odate param id';

comment on column jcm_schedule.f_syscode is 'System code';

comment on column jcm_schedule.f_createtime is 'create time';

comment on column jcm_schedule.f_updatetime is 'update time';

comment on column jcm_schedule.f_dickey is 'Business classification key';

alter table jcm_schedule
    owner to postgres;

create table jcm_relation
(
    f_id            varchar(100) not null
        primary key,
    f_scheduleid    varchar(255) not null,
    f_flowid        varchar(255) not null,
    f_type          varchar(255),
    f_priority      varchar(255),
    f_schedulername varchar(255)
);

comment on table jcm_relation is 'relation table';

comment on column jcm_relation.f_id is 'Primary key';

comment on column jcm_relation.f_scheduleid is 'schedule id';

comment on column jcm_relation.f_flowid is 'flow id';

comment on column jcm_relation.f_type is 'type';

comment on column jcm_relation.f_priority is 'priority';

comment on column jcm_relation.f_schedulername is 'scheduler name';

alter table jcm_relation
    owner to postgres;

create table jcm_condition
(
    f_id          varchar(100) not null
        primary key,
    f_name        varchar(255) not null,
    f_alias       varchar(255),
    f_desc        varchar(1000),
    f_label       varchar(255),
    f_type        varchar(255) not null,
    f_valid       varchar(255) not null,
    f_messageid   varchar(255),
    f_starttime   timestamp,
    f_endtime     timestamp,
    f_checkbdate  varchar(255),
    f_field       varchar(255),
    f_fieldtrue   varchar(255),
    f_fieldfaulse varchar(255),
    f_markfield   varchar(255),
    f_syscode     varchar(255),
    f_createtime  timestamp,
    f_updatetime  timestamp,
    f_dickey      varchar(1200)
);

comment on table jcm_condition is 'condition table';

comment on column jcm_condition.f_id is 'Primary key';

comment on column jcm_condition.f_name is 'name';

comment on column jcm_condition.f_alias is 'alias';

comment on column jcm_condition.f_desc is 'desc';

comment on column jcm_condition.f_label is 'label';

comment on column jcm_condition.f_type is 'type';

comment on column jcm_condition.f_valid is 'valid';

comment on column jcm_condition.f_messageid is 'message id';

comment on column jcm_condition.f_starttime is 'start time';

comment on column jcm_condition.f_endtime is 'end time';

comment on column jcm_condition.f_checkbdate is 'checkb date';

comment on column jcm_condition.f_field is 'field';

comment on column jcm_condition.f_fieldtrue is 'field true';

comment on column jcm_condition.f_fieldfaulse is 'field false';

comment on column jcm_condition.f_markfield is 'markfield';

comment on column jcm_condition.f_syscode is 'System code';

comment on column jcm_condition.f_createtime is 'create time';

comment on column jcm_condition.f_updatetime is 'update time';

comment on column jcm_condition.f_dickey is 'Business classification key';

alter table jcm_condition
    owner to postgres;

create table jcm_flowcondition
(
    f_id            varchar(100) not null
        primary key,
    f_conditionid   varchar(255) not null,
    f_jobflowid     varchar(255) not null,
    f_type          varchar(255),
    f_priority      varchar(255),
    f_schedulername varchar(255)
);

comment on table jcm_flowcondition is 'flow condition table';

comment on column jcm_flowcondition.f_id is 'Primary key';

comment on column jcm_flowcondition.f_conditionid is 'event id';

comment on column jcm_flowcondition.f_jobflowid is 'flow id';

comment on column jcm_flowcondition.f_type is 'type';

comment on column jcm_flowcondition.f_priority is 'priority';

comment on column jcm_flowcondition.f_schedulername is 'scheduler name';

alter table jcm_flowcondition
    owner to postgres;

create table jcm_event_param
(
    f_id         varchar(100) not null
        primary key,
    f_eventflag  varchar(255),
    f_paramname  varchar(255),
    f_paramvalue varchar(255),
    f_parammark  varchar(255)
);

comment on table jcm_event_param is 'event param table';

comment on column jcm_event_param.f_id is 'Primary key';

comment on column jcm_event_param.f_eventflag is 'event flag';

comment on column jcm_event_param.f_paramname is 'param name';

comment on column jcm_event_param.f_paramvalue is 'param value';

comment on column jcm_event_param.f_parammark is 'param mark';

alter table jcm_event_param
    owner to postgres;

create table jcm_conditionrecord
(
    f_id              varchar(100) not null
        primary key,
    f_eventid         varchar(255),
    f_markfieldvalue  varchar(255),
    f_readyfieldvalue varchar(255),
    f_generatetime    timestamp
);

comment on table jcm_conditionrecord is 'condition record table';

comment on column jcm_conditionrecord.f_id is 'Primary key';

comment on column jcm_conditionrecord.f_eventid is 'event id';

comment on column jcm_conditionrecord.f_markfieldvalue is 'mark field value';

comment on column jcm_conditionrecord.f_readyfieldvalue is 'ready field value';

comment on column jcm_conditionrecord.f_generatetime is 'generate time';

alter table jcm_conditionrecord
    owner to postgres;

create table jcm_schedevent_param
(
    f_id           varchar(100) not null
        primary key,
    f_schedeventid varchar(255),
    f_paramid      varchar(255),
    f_type         varchar(255)
);

comment on table jcm_schedevent_param is 'schedevent param table';

comment on column jcm_schedevent_param.f_id is 'Primary key';

comment on column jcm_schedevent_param.f_schedeventid is 'schedevent id';

comment on column jcm_schedevent_param.f_paramid is 'param id';

comment on column jcm_schedevent_param.f_type is 'param type';

alter table jcm_schedevent_param
    owner to postgres;

create table jcm_scheduleinfo
(
    f_id             varchar(255) not null
        primary key,
    f_scheduleid     varchar(255) not null,
    f_schedulename   varchar(255),
    f_lastlaunchtime varchar(255),
    f_nextlaunchtime varchar(255) not null
);

comment on table jcm_scheduleinfo is 'schedule info table';

comment on column jcm_scheduleinfo.f_id is 'Primary key';

comment on column jcm_scheduleinfo.f_scheduleid is 'schedule id';

comment on column jcm_scheduleinfo.f_schedulename is 'name';

comment on column jcm_scheduleinfo.f_lastlaunchtime is 'schedule last launch time';

comment on column jcm_scheduleinfo.f_nextlaunchtime is 'schedule next launch time';

alter table jcm_scheduleinfo
    owner to postgres;

create table jcm_schconilog
(
    f_id            varchar(255) not null
        primary key,
    f_sourceid      varchar(255) not null,
    f_sourcetype    varchar(255) not null,
    f_xjobid        varchar(255),
    f_xjobtype      varchar(255),
    f_jfinstancedid varchar(255),
    f_status        varchar(255),
    f_date          timestamp,
    f_message       varchar(255),
    f_instid        varchar(255),
    f_time          varchar(255),
    f_xjobname      varchar(255),
    f_sourcename    varchar(255)
);

comment on table jcm_schconilog is 'schedule/event log table';

comment on column jcm_schconilog.f_id is 'Primary key';

comment on column jcm_schconilog.f_sourceid is 'source ID';

comment on column jcm_schconilog.f_sourcetype is 'source type';

comment on column jcm_schconilog.f_xjobid is 'job/flow ID';

comment on column jcm_schconilog.f_xjobtype is 'type';

comment on column jcm_schconilog.f_jfinstancedid is 'flow instance ID';

comment on column jcm_schconilog.f_status is 'status';

comment on column jcm_schconilog.f_date is 'execution time';

comment on column jcm_schconilog.f_message is 'message';

comment on column jcm_schconilog.f_instid is 'schedule/event instance id';

comment on column jcm_schconilog.f_time is 'time';

comment on column jcm_schconilog.f_xjobname is 'job name';

comment on column jcm_schconilog.f_sourcename is 'source name';

alter table jcm_schconilog
    owner to postgres;

create table jcm_schconiloghis
(
    f_id            varchar(255) not null
        primary key,
    f_sourceid      varchar(255) not null,
    f_sourcetype    varchar(255) not null,
    f_xjobid        varchar(255),
    f_xjobtype      varchar(255),
    f_jfinstancedid varchar(255),
    f_status        varchar(255),
    f_date          timestamp,
    f_message       varchar(255),
    f_instid        varchar(255),
    f_time          varchar(255),
    f_xjobname      varchar(255),
    f_sourcename    varchar(255)
);

comment on table jcm_schconiloghis is 'schedule/event historical log table';

comment on column jcm_schconiloghis.f_id is 'Primary key';

comment on column jcm_schconiloghis.f_sourceid is 'source ID';

comment on column jcm_schconiloghis.f_sourcetype is 'source type';

comment on column jcm_schconiloghis.f_xjobid is 'job/flow ID';

comment on column jcm_schconiloghis.f_xjobtype is 'type';

comment on column jcm_schconiloghis.f_jfinstancedid is 'flow instance ID';

comment on column jcm_schconiloghis.f_status is 'status';

comment on column jcm_schconiloghis.f_date is 'execution time';

comment on column jcm_schconiloghis.f_message is 'message';

comment on column jcm_schconiloghis.f_instid is 'schedule/event instance id';

comment on column jcm_schconiloghis.f_time is 'time';

comment on column jcm_schconiloghis.f_xjobname is 'job name';

comment on column jcm_schconiloghis.f_sourcename is 'source name';

alter table jcm_schconiloghis
    owner to postgres;

create table jcm_eventexe_control
(
    f_id        varchar(100) not null
        primary key,
    f_appid     varchar(255),
    f_writedate date,
    f_label     varchar(255),
    f_syscode   varchar(255)
);

comment on table jcm_eventexe_control is 'Event Execution Control Table';

comment on column jcm_eventexe_control.f_id is 'Primary key';

comment on column jcm_eventexe_control.f_appid is 'The machine ID';

comment on column jcm_eventexe_control.f_writedate is 'write time';

comment on column jcm_eventexe_control.f_label is 'label';

comment on column jcm_eventexe_control.f_syscode is 'System code';

alter table jcm_eventexe_control
    owner to postgres;

create table jcm_flopconfig
(
    f_id         varchar(50) not null
        primary key,
    f_name       varchar(255),
    f_alias      varchar(255),
    f_desc       varchar(255),
    f_type       varchar(255),
    f_strategy   integer,
    f_calendar   varchar(255),
    f_createtime timestamp,
    f_updatetime timestamp
);

comment on table jcm_flopconfig is 'flop config';

comment on column jcm_flopconfig.f_id is 'primary key';

comment on column jcm_flopconfig.f_name is 'flop name';

comment on column jcm_flopconfig.f_alias is 'flop alias';

comment on column jcm_flopconfig.f_desc is 'desc';

comment on column jcm_flopconfig.f_type is 'flop type';

comment on column jcm_flopconfig.f_strategy is 'flop strategy';

comment on column jcm_flopconfig.f_calendar is 'flop calendar';

comment on column jcm_flopconfig.f_createtime is 'create time';

comment on column jcm_flopconfig.f_updatetime is 'update time';

alter table jcm_flopconfig
    owner to postgres;

create table jcm_jobflowstatus
(
    f_id             varchar(50) not null
        primary key,
    f_jobflowid      varchar(50),
    f_jobflowname    varchar(255),
    f_status         varchar(30),
    f_syscode        varchar(50),
    f_odate          varchar(12),
    f_jfinstancedid  varchar(50),
    f_duration       varchar(255),
    f_starttime      timestamp,
    f_endtime        timestamp,
    f_parentid       varchar(50),
    f_parentinstid   varchar(50),
    f_sourcetype     varchar(100),
    f_sourceid       varchar(50),
    f_sourcename     varchar(200),
    f_dir            varchar(200),
    f_desc           varchar(255),
    f_rootid         varchar(50),
    f_rootinstid     varchar(50),
    f_nexttime       timestamp,
    f_durationmillis integer,
    f_dickey         varchar(1200),
    f_variance       integer
);

comment on table jcm_jobflowstatus is 'Job flow execution log table';

comment on column jcm_jobflowstatus.f_id is 'Primary key';

comment on column jcm_jobflowstatus.f_jobflowid is 'Job flow id';

comment on column jcm_jobflowstatus.f_jobflowname is 'Job flow Name';

comment on column jcm_jobflowstatus.f_status is 'job flow status';

comment on column jcm_jobflowstatus.f_syscode is 'system code';

comment on column jcm_jobflowstatus.f_odate is 'odate';

comment on column jcm_jobflowstatus.f_jfinstancedid is 'Job stream instance ID';

comment on column jcm_jobflowstatus.f_duration is 'Execution time';

comment on column jcm_jobflowstatus.f_starttime is 'Start time';

comment on column jcm_jobflowstatus.f_endtime is 'End time';

comment on column jcm_jobflowstatus.f_parentid is 'Parent id';

comment on column jcm_jobflowstatus.f_parentinstid is 'Parent flow instance ID';

comment on column jcm_jobflowstatus.f_sourcetype is 'Source type';

comment on column jcm_jobflowstatus.f_sourceid is 'Source type id';

comment on column jcm_jobflowstatus.f_sourcename is 'Source type name';

comment on column jcm_jobflowstatus.f_dir is 'Dir';

comment on column jcm_jobflowstatus.f_desc is 'Desc';

comment on column jcm_jobflowstatus.f_rootid is 'Root id';

comment on column jcm_jobflowstatus.f_rootinstid is 'Root flow instance ID';

comment on column jcm_jobflowstatus.f_nexttime is 'Next time';

comment on column jcm_jobflowstatus.f_durationmillis is 'Execution time';

comment on column jcm_jobflowstatus.f_dickey is 'Business classification key';

comment on column jcm_jobflowstatus.f_variance is 'The variance';

alter table jcm_jobflowstatus
    owner to postgres;

create table jcm_jobflowstatushis
(
    f_id             varchar(50) not null
        primary key,
    f_jobflowid      varchar(50),
    f_jobflowname    varchar(255),
    f_status         varchar(30),
    f_syscode        varchar(50),
    f_odate          varchar(12),
    f_jfinstancedid  varchar(50),
    f_duration       varchar(255),
    f_starttime      timestamp,
    f_endtime        timestamp,
    f_statusflag     varchar(40),
    f_parentid       varchar(50),
    f_parentinstid   varchar(50),
    f_sourcetype     varchar(100),
    f_sourceid       varchar(50),
    f_sourcename     varchar(200),
    f_dir            varchar(200),
    f_desc           varchar(255),
    f_rootid         varchar(50),
    f_rootinstid     varchar(50),
    f_nexttime       timestamp,
    f_durationmillis integer,
    f_dickey         varchar(1200),
    f_variance       integer
);

comment on table jcm_jobflowstatushis is 'Job flow execution log table';

comment on column jcm_jobflowstatushis.f_id is 'Primary key';

comment on column jcm_jobflowstatushis.f_jobflowid is 'Job flow id';

comment on column jcm_jobflowstatushis.f_jobflowname is 'Job flow Name';

comment on column jcm_jobflowstatushis.f_status is 'job flow status';

comment on column jcm_jobflowstatushis.f_syscode is 'system code';

comment on column jcm_jobflowstatushis.f_odate is 'odate';

comment on column jcm_jobflowstatushis.f_jfinstancedid is 'Job stream instance ID';

comment on column jcm_jobflowstatushis.f_duration is 'Execution time';

comment on column jcm_jobflowstatushis.f_starttime is 'Start time';

comment on column jcm_jobflowstatushis.f_endtime is 'End time';

comment on column jcm_jobflowstatushis.f_statusflag is 'Status flag';

comment on column jcm_jobflowstatushis.f_parentid is 'Parent id';

comment on column jcm_jobflowstatushis.f_parentinstid is 'Parent flow instance ID';

comment on column jcm_jobflowstatushis.f_sourcetype is 'Source type';

comment on column jcm_jobflowstatushis.f_sourceid is 'Source type id';

comment on column jcm_jobflowstatushis.f_sourcename is 'Source type name';

comment on column jcm_jobflowstatushis.f_dir is 'Dir';

comment on column jcm_jobflowstatushis.f_desc is 'Desc';

comment on column jcm_jobflowstatushis.f_rootid is 'Root id';

comment on column jcm_jobflowstatushis.f_rootinstid is 'Root flow instance ID';

comment on column jcm_jobflowstatushis.f_nexttime is 'Next time';

comment on column jcm_jobflowstatushis.f_durationmillis is 'Execution time';

comment on column jcm_jobflowstatushis.f_dickey is 'Business classification key';

comment on column jcm_jobflowstatushis.f_variance is 'The variance';

alter table jcm_jobflowstatushis
    owner to postgres;

create table jcm_jobstatus
(
    f_id                varchar(50) not null
        primary key,
    f_jobid             varchar(50),
    f_jobname           varchar(255),
    f_type              varchar(50),
    f_jobflowid         varchar(50),
    f_jobflowname       varchar(255),
    f_status            varchar(30),
    f_syscode           varchar(50),
    f_odate             varchar(12),
    f_jobflowinstanceid varchar(50),
    f_duration          varchar(255),
    f_starttime         timestamp,
    f_endtime           timestamp,
    f_agentname         varchar(100),
    f_schedulername     varchar(100),
    f_dir               varchar(200),
    f_parentjob         varchar(100),
    f_afterjob          varchar(100),
    f_source            varchar(50),
    f_sourceid          varchar(50),
    f_sourcetype        varchar(50),
    f_sourcename        varchar(100),
    f_definetype        varchar(50),
    f_target            varchar(20),
    f_log               varchar(4000),
    f_desc              varchar(500),
    f_durationmillis    integer,
    f_debug             varchar(255),
    f_dickey            varchar(1200)
);

comment on table jcm_jobstatus is 'Job status';

comment on column jcm_jobstatus.f_id is 'Primary key';

comment on column jcm_jobstatus.f_jobid is 'Job id';

comment on column jcm_jobstatus.f_jobname is 'Job name';

comment on column jcm_jobstatus.f_type is 'Job type';

comment on column jcm_jobstatus.f_jobflowid is 'Job flow id';

comment on column jcm_jobstatus.f_jobflowname is 'Job flow Name';

comment on column jcm_jobstatus.f_status is 'Job status';

comment on column jcm_jobstatus.f_syscode is 'system code';

comment on column jcm_jobstatus.f_odate is 'odate';

comment on column jcm_jobstatus.f_jobflowinstanceid is 'Job stream instance ID';

comment on column jcm_jobstatus.f_duration is 'Execution time';

comment on column jcm_jobstatus.f_starttime is 'Start time';

comment on column jcm_jobstatus.f_endtime is 'End time';

comment on column jcm_jobstatus.f_agentname is 'Proxy server';

comment on column jcm_jobstatus.f_schedulername is 'Dispatch server';

comment on column jcm_jobstatus.f_dir is 'Dir';

comment on column jcm_jobstatus.f_parentjob is 'Parent job';

comment on column jcm_jobstatus.f_afterjob is 'Follow-up';

comment on column jcm_jobstatus.f_source is 'Source';

comment on column jcm_jobstatus.f_sourceid is 'Source type id';

comment on column jcm_jobstatus.f_sourcetype is 'Source type';

comment on column jcm_jobstatus.f_sourcename is 'Source type name';

comment on column jcm_jobstatus.f_definetype is 'Definition type';

comment on column jcm_jobstatus.f_target is 'Target';

comment on column jcm_jobstatus.f_log is 'Log';

comment on column jcm_jobstatus.f_desc is 'Desc';

comment on column jcm_jobstatus.f_durationmillis is 'Execution time';

comment on column jcm_jobstatus.f_debug is 'debug';

comment on column jcm_jobstatus.f_dickey is 'Business classification key';

alter table jcm_jobstatus
    owner to postgres;

create table jcm_jobstatushis
(
    f_id                varchar(50) not null
        primary key,
    f_jobid             varchar(50),
    f_jobname           varchar(255),
    f_type              varchar(50),
    f_jobflowid         varchar(50),
    f_jobflowname       varchar(255),
    f_status            varchar(30),
    f_syscode           varchar(50),
    f_odate             varchar(12),
    f_jobflowinstanceid varchar(50),
    f_duration          varchar(255),
    f_starttime         timestamp,
    f_endtime           timestamp,
    f_agentname         varchar(100),
    f_statusflag        varchar(100),
    f_schedulername     varchar(100),
    f_dir               varchar(200),
    f_parentjob         varchar(100),
    f_afterjob          varchar(100),
    f_source            varchar(50),
    f_sourceid          varchar(50),
    f_sourcetype        varchar(50),
    f_sourcename        varchar(100),
    f_definetype        varchar(50),
    f_target            varchar(20),
    f_log               varchar(4000),
    f_desc              varchar(500),
    f_durationmillis    integer,
    f_dickey            varchar(1200)
);

comment on table jcm_jobstatushis is 'Job status history table';

comment on column jcm_jobstatushis.f_id is 'Primary key';

comment on column jcm_jobstatushis.f_jobid is 'Job id';

comment on column jcm_jobstatushis.f_jobname is 'Job name';

comment on column jcm_jobstatushis.f_type is 'Job type';

comment on column jcm_jobstatushis.f_jobflowid is 'Job flow id';

comment on column jcm_jobstatushis.f_jobflowname is 'Job flow Name';

comment on column jcm_jobstatushis.f_status is 'Job status';

comment on column jcm_jobstatushis.f_syscode is 'system code';

comment on column jcm_jobstatushis.f_odate is 'odate';

comment on column jcm_jobstatushis.f_jobflowinstanceid is 'Job stream instance ID';

comment on column jcm_jobstatushis.f_duration is 'Execution time';

comment on column jcm_jobstatushis.f_starttime is 'Start time';

comment on column jcm_jobstatushis.f_endtime is 'End time';

comment on column jcm_jobstatushis.f_agentname is 'Proxy server';

comment on column jcm_jobstatushis.f_statusflag is 'Status flag';

comment on column jcm_jobstatushis.f_schedulername is 'Dispatch server';

comment on column jcm_jobstatushis.f_dir is 'Dir';

comment on column jcm_jobstatushis.f_parentjob is 'Parent job';

comment on column jcm_jobstatushis.f_afterjob is 'Follow-up';

comment on column jcm_jobstatushis.f_source is 'Source';

comment on column jcm_jobstatushis.f_sourceid is 'Source type id';

comment on column jcm_jobstatushis.f_sourcetype is 'Source type';

comment on column jcm_jobstatushis.f_sourcename is 'Source type name';

comment on column jcm_jobstatushis.f_definetype is 'Definition type';

comment on column jcm_jobstatushis.f_target is 'Target';

comment on column jcm_jobstatushis.f_log is 'Log';

comment on column jcm_jobstatushis.f_desc is 'Desc';

comment on column jcm_jobstatushis.f_durationmillis is 'Execution time';

comment on column jcm_jobstatushis.f_dickey is 'Business classification key';

alter table jcm_jobstatushis
    owner to postgres;

create table jcm_onekeyrun
(
    f_id        varchar(33) not null
        primary key,
    f_xjobid    varchar(33) not null,
    f_prexjobid varchar(33) not null,
    f_sign      varchar(5),
    f_condtype  varchar(50),
    f_value     varchar(100),
    f_odate     varchar(20)
);

comment on table jcm_onekeyrun is 'One-click rerun temporary table';

comment on column jcm_onekeyrun.f_id is 'Primary key';

comment on column jcm_onekeyrun.f_xjobid is 'Job Flow Id';

comment on column jcm_onekeyrun.f_prexjobid is 'Predecessor ID';

comment on column jcm_onekeyrun.f_sign is 'Predecessor completion indicator';

comment on column jcm_onekeyrun.f_condtype is 'Predecessor status';

comment on column jcm_onekeyrun.f_value is 'Triggering conditions';

comment on column jcm_onekeyrun.f_odate is 'odate';

alter table jcm_onekeyrun
    owner to postgres;

create table jcm_business_date
(
    f_id                     varchar(50) not null
        primary key,
    f_name                   varchar(255),
    f_alias                  varchar(255),
    f_initial_business_date  date,
    f_business_date          date,
    f_previous_business_date date,
    f_rule_id                varchar(255),
    f_type                   varchar(255),
    f_modify_date            date
);

comment on table jcm_business_date is 'business date definition table';

comment on column jcm_business_date.f_id is 'primary key';

comment on column jcm_business_date.f_name is 'business date name';

comment on column jcm_business_date.f_alias is 'business date alias';

comment on column jcm_business_date.f_initial_business_date is 'initialization date';

comment on column jcm_business_date.f_business_date is 'business date';

comment on column jcm_business_date.f_previous_business_date is 'last business date';

comment on column jcm_business_date.f_rule_id is 'rule ID';

comment on column jcm_business_date.f_type is 'type';

comment on column jcm_business_date.f_modify_date is 'modification time';

alter table jcm_business_date
    owner to postgres;

create table jcm_callbackinfo
(
    f_id              varchar(50) not null
        primary key,
    f_jobflowid       varchar(255),
    f_flowinstancedid varchar(255),
    f_jobid           varchar(255),
    f_retcode         varchar(255),
    f_status          varchar(255),
    f_paravalues      varchar(4000),
    f_agentserverid   varchar(255)
);

comment on table jcm_callbackinfo is 'job flow, job execution feedback information table';

comment on column jcm_callbackinfo.f_id is 'primary key';

comment on column jcm_callbackinfo.f_jobflowid is 'job flow definition ID';

comment on column jcm_callbackinfo.f_flowinstancedid is 'stream instance ID';

comment on column jcm_callbackinfo.f_jobid is 'job / stream ID';

comment on column jcm_callbackinfo.f_retcode is 'return code';

comment on column jcm_callbackinfo.f_status is 'status';

comment on column jcm_callbackinfo.f_paravalues is 'parameter information';

comment on column jcm_callbackinfo.f_agentserverid is 'proxy server ID';

alter table jcm_callbackinfo
    owner to postgres;

create table jcm_eventplan_control
(
    f_id            varchar(50) not null
        primary key,
    f_applicationid varchar(255),
    f_updatetime    date
);

comment on table jcm_eventplan_control is 'schedule, event polling, lock control table';

comment on column jcm_eventplan_control.f_id is 'primary key';

comment on column jcm_eventplan_control.f_applicationid is 'integrated environment application ID';

comment on column jcm_eventplan_control.f_updatetime is 'update time';

alter table jcm_eventplan_control
    owner to postgres;

create table jcm_execute_control
(
    f_flowid   varchar(50) not null,
    f_childid  varchar(50) not null,
    f_odate    varchar(50) not null,
    f_retrynum integer,
    f_personid varchar(255),
    f_ext1     varchar(255),
    primary key (f_flowid, f_childid, f_odate)
);

comment on table jcm_execute_control is 'new job flow execution control table';

comment on column jcm_execute_control.f_flowid is 'job flow ID';

comment on column jcm_execute_control.f_odate is 'data date';

comment on column jcm_execute_control.f_retrynum is 'record number of retries';

comment on column jcm_execute_control.f_personid is 'corporate ID';

comment on column jcm_execute_control.f_ext1 is 'reserved field';

alter table jcm_execute_control
    owner to postgres;

create table jcm_flowparas
(
    f_id              varchar(50)  not null
        primary key,
    f_jobflowstatusid varchar(255) not null,
    f_pname           varchar(255) not null,
    f_pvalue          varchar(1000),
    f_time            timestamp
);

comment on table jcm_flowparas is 'flow parameter table';

comment on column jcm_flowparas.f_id is 'primary key';

comment on column jcm_flowparas.f_jobflowstatusid is 'stream running status number';

comment on column jcm_flowparas.f_pname is 'parameter variable name';

comment on column jcm_flowparas.f_pvalue is 'parameter value';

comment on column jcm_flowparas.f_time is 'time';

alter table jcm_flowparas
    owner to postgres;

create table jcm_flowretry_execute
(
    f_flowid    varchar(50) not null,
    f_firstnode varchar(50) not null,
    f_odate     varchar(50) not null,
    f_personid  varchar(255),
    f_ext1      varchar(255),
    primary key (f_flowid, f_firstnode, f_odate)
);

comment on table jcm_flowretry_execute is 'new job flow retry control table';

comment on column jcm_flowretry_execute.f_flowid is 'job flow ID';

comment on column jcm_flowretry_execute.f_firstnode is 'first try record';

comment on column jcm_flowretry_execute.f_odate is 'data date';

comment on column jcm_flowretry_execute.f_personid is 'corporate ID';

comment on column jcm_flowretry_execute.f_ext1 is 'reserved field';

alter table jcm_flowretry_execute
    owner to postgres;

create table jcm_flowretry_waitqueue
(
    f_flowid            varchar(50) not null,
    f_odate             varchar(50) not null,
    f_jobflowinstanceid varchar(255),
    f_updatetime        timestamp,
    f_cmdid             varchar(255),
    primary key (f_flowid, f_odate)
);

comment on table jcm_flowretry_waitqueue is 'stream retry queue list';

comment on column jcm_flowretry_waitqueue.f_flowid is 'job flow ID';

comment on column jcm_flowretry_waitqueue.f_odate is 'data date';

comment on column jcm_flowretry_waitqueue.f_jobflowinstanceid is 'stream instance ID';

comment on column jcm_flowretry_waitqueue.f_updatetime is 'update time';

comment on column jcm_flowretry_waitqueue.f_cmdid is 'ID corresponding to ICommand';

alter table jcm_flowretry_waitqueue
    owner to postgres;

create table jcm_job_business_date
(
    f_id               varchar(50) not null
        primary key,
    f_xjob_id          varchar(50) not null,
    f_name             varchar(255),
    f_job_type         varchar(255),
    f_config_status    varchar(255),
    f_is_changed       varchar(255),
    f_final_status     varchar(255),
    f_modify_date      date,
    f_business_date_id varchar(255)
);

comment on table jcm_job_business_date is 'business date association table';

comment on column jcm_job_business_date.f_id is 'primary key';

comment on column jcm_job_business_date.f_xjob_id is 'job / stream ID';

comment on column jcm_job_business_date.f_name is 'business date name';

comment on column jcm_job_business_date.f_job_type is 'type';

comment on column jcm_job_business_date.f_config_status is 'configuration status';

comment on column jcm_job_business_date.f_is_changed is 'whether to overturn';

comment on column jcm_job_business_date.f_final_status is 'status';

comment on column jcm_job_business_date.f_modify_date is 'modification time';

comment on column jcm_job_business_date.f_business_date_id is 'business date definition ID';

alter table jcm_job_business_date
    owner to postgres;

create table jcm_jobwait_queue
(
    f_flowid            varchar(50) not null,
    f_jobid             varchar(50) not null,
    f_odate             varchar(50) not null,
    f_jobflowinstanceid varchar(255),
    f_nodetype          varchar(255),
    f_updatetime        timestamp,
    f_appid             varchar(255),
    f_cmdid             varchar(255),
    primary key (f_flowid, f_jobid, f_odate)
);

comment on column jcm_jobwait_queue.f_flowid is 'job flow ID';

comment on column jcm_jobwait_queue.f_jobid is 'job / stream ID';

comment on column jcm_jobwait_queue.f_odate is 'data date';

comment on column jcm_jobwait_queue.f_jobflowinstanceid is 'stream instance ID';

comment on column jcm_jobwait_queue.f_updatetime is 'update time';

comment on column jcm_jobwait_queue.f_cmdid is 'ID corresponding to ICommand';

alter table jcm_jobwait_queue
    owner to postgres;

create table jcm_jobwakeup_control
(
    f_id            varchar(50)  not null
        primary key,
    f_applicationid varchar(255) not null,
    f_updatetime    timestamp
);

comment on table jcm_jobwakeup_control is 'Job wake up resource information table';

comment on column jcm_jobwakeup_control.f_id is 'primary key';

comment on column jcm_jobwakeup_control.f_applicationid is 'integrated environment application ID';

comment on column jcm_jobwakeup_control.f_updatetime is 'update time';

alter table jcm_jobwakeup_control
    owner to postgres;

create table jcm_listenerserver
(
    f_id              varchar(50)  not null
        primary key,
    f_listenername    varchar(255) not null,
    f_alias           varchar(255),
    f_desc            varchar(255),
    f_schedulerserver varchar(255),
    f_listenerport    varchar(255)
);

comment on table jcm_listenerserver is 'listen to server table';

comment on column jcm_listenerserver.f_id is 'primary key';

comment on column jcm_listenerserver.f_listenername is 'listen server name';

comment on column jcm_listenerserver.f_alias is 'listener alias';

comment on column jcm_listenerserver.f_desc is 'listen to server description';

comment on column jcm_listenerserver.f_schedulerserver is 'scheduling server URL';

comment on column jcm_listenerserver.f_listenerport is 'listen port';

alter table jcm_listenerserver
    owner to postgres;

create table jcm_odateexpress
(
    f_id         varchar(50)  not null
        primary key,
    f_xjobid     varchar(255) not null,
    f_prexjobid  varchar(255),
    f_pretype    varchar(255),
    f_postxjobid varchar(255),
    f_posttype   varchar(255),
    f_sign       varchar(255),
    f_value      varchar(255)
);

comment on table jcm_odateexpress is 'date expression table';

comment on column jcm_odateexpress.f_id is 'primary key';

comment on column jcm_odateexpress.f_xjobid is 'job / stream definition ID';

comment on column jcm_odateexpress.f_pretype is 'predecessor object type';

comment on column jcm_odateexpress.f_postxjobid is 'current object ID';

comment on column jcm_odateexpress.f_posttype is 'current object type';

comment on column jcm_odateexpress.f_sign is 'symbol';

comment on column jcm_odateexpress.f_value is 'value';

alter table jcm_odateexpress
    owner to postgres;

create table jcm_predeal_control
(
    f_flowid   varchar(50)  not null,
    f_odate    varchar(255) not null,
    f_personid varchar(255),
    f_ext1     integer,
    primary key (f_flowid, f_odate)
);

comment on table jcm_predeal_control is 'Pre trigger control table';

comment on column jcm_predeal_control.f_flowid is 'job flow ID';

comment on column jcm_predeal_control.f_odate is 'data date';

comment on column jcm_predeal_control.f_personid is 'corporate ID';

comment on column jcm_predeal_control.f_ext1 is 'reserved field';

alter table jcm_predeal_control
    owner to postgres;

create table jcm_retry_control
(
    f_flowid    varchar(50)  not null,
    f_odate     varchar(255) not null,
    f_retrydate timestamp,
    f_retrynum  integer,
    f_personid  varchar(255),
    f_ext1      varchar(255),
    primary key (f_flowid, f_odate)
);

comment on table jcm_retry_control is 'Job flow retry control table';

comment on column jcm_retry_control.f_flowid is 'job flow ID';

comment on column jcm_retry_control.f_odate is 'data date';

comment on column jcm_retry_control.f_retrydate is 'Retrying start time';

comment on column jcm_retry_control.f_retrynum is 'record number of retries';

comment on column jcm_retry_control.f_personid is 'corporate ID';

comment on column jcm_retry_control.f_ext1 is 'reserved field';

alter table jcm_retry_control
    owner to postgres;

create table jcm_rule
(
    f_id          varchar(50)  not null
        primary key,
    f_name        varchar(255) not null,
    f_alias       varchar(255),
    f_description varchar(255),
    f_step        integer,
    f_calendarid  varchar(255),
    f_frequencyid varchar(255)
);

comment on table jcm_rule is 'rule table';

comment on column jcm_rule.f_id is 'primary key';

comment on column jcm_rule.f_name is 'rule name';

comment on column jcm_rule.f_alias is 'rule alias';

comment on column jcm_rule.f_description is 'rule description';

comment on column jcm_rule.f_step is 'step';

comment on column jcm_rule.f_calendarid is 'calendar ID';

comment on column jcm_rule.f_frequencyid is 'Frequency ID';

alter table jcm_rule
    owner to postgres;

create table jcm_schedulelog
(
    f_id         varchar(50)  not null
        primary key,
    f_flowid     varchar(255) not null,
    f_jobid      varchar(255),
    f_scheduleid varchar(255),
    f_action     varchar(255),
    f_status     varchar(255),
    f_logtime    timestamp,
    f_logtext    varchar(255)
);

comment on table jcm_schedulelog is 'scheduling log table';

comment on column jcm_schedulelog.f_id is 'primary key';

comment on column jcm_schedulelog.f_flowid is 'job flow ID';

comment on column jcm_schedulelog.f_jobid is 'job / stream ID';

comment on column jcm_schedulelog.f_scheduleid is 'scheduling server ID';

comment on column jcm_schedulelog.f_action is 'action';

comment on column jcm_schedulelog.f_status is 'execution status';

comment on column jcm_schedulelog.f_logtime is 'record time';

comment on column jcm_schedulelog.f_logtext is 'details';

alter table jcm_schedulelog
    owner to postgres;

create table jcm_schedule_opt
(
    f_id      varchar(50)  not null
        primary key,
    f_schid   varchar(255) not null,
    f_opt     varchar(255),
    f_opttime timestamp
);

comment on table jcm_schedule_opt is 'intermediate table of planned operation records';

comment on column jcm_schedule_opt.f_id is 'primary key';

comment on column jcm_schedule_opt.f_schid is 'plan ID';

comment on column jcm_schedule_opt.f_opt is 'operation type';

comment on column jcm_schedule_opt.f_opttime is 'operation time';

alter table jcm_schedule_opt
    owner to postgres;

create table jcm_sms
(
    f_id        varchar(50)  not null
        primary key,
    f_teleno    varchar(255) not null,
    f_msg       varchar(255),
    f_date      varchar(255),
    f_status    varchar(255),
    f_messageid varchar(255)
);

comment on table jcm_sms is 'SMS message table';

comment on column jcm_sms.f_id is 'primary key';

comment on column jcm_sms.f_teleno is 'phone number';

comment on column jcm_sms.f_msg is 'send message content';

comment on column jcm_sms.f_date is 'sending date';

comment on column jcm_sms.f_status is 'execution status';

comment on column jcm_sms.f_messageid is 'message configuration ID';

alter table jcm_sms
    owner to postgres;

create table jcm_threadmonitor
(
    f_id         varchar(50)  not null
        primary key,
    f_name       varchar(255) not null,
    f_alias      varchar(255),
    f_ip         varchar(255),
    f_port       varchar(255),
    f_appid      varchar(255),
    f_tasknumber varchar(255),
    f_status     varchar(255),
    f_order      integer,
    f_log        varchar(255),
    f_createdate timestamp,
    f_updatedate timestamp
);

comment on table jcm_threadmonitor is 'thread monitor table';

comment on column jcm_threadmonitor.f_id is 'primary key';

comment on column jcm_threadmonitor.f_name is 'thread name';

comment on column jcm_threadmonitor.f_alias is 'thread alias';

comment on column jcm_threadmonitor.f_ip is 'machine IP';

comment on column jcm_threadmonitor.f_port is 'application port';

comment on column jcm_threadmonitor.f_appid is 'machine ID';

comment on column jcm_threadmonitor.f_tasknumber is 'number of tasks on current node';

comment on column jcm_threadmonitor.f_status is 'execution status';

comment on column jcm_threadmonitor.f_order is 'sort';

comment on column jcm_threadmonitor.f_log is 'thread log';

comment on column jcm_threadmonitor.f_createdate is 'creation time';

comment on column jcm_threadmonitor.f_updatedate is 'update time';

alter table jcm_threadmonitor
    owner to postgres;

create table jcm_threadmonitorlog
(
    f_id        varchar(50)  not null
        primary key,
    f_threadid  varchar(255) not null,
    f_ip        varchar(255),
    f_port      varchar(255),
    f_status    varchar(255),
    f_writedate timestamp,
    f_log       varchar(4000),
    f_read      varchar(255)
);

comment on table jcm_threadmonitorlog is 'thread monitoring log table';

comment on column jcm_threadmonitorlog.f_id is 'primary key';

comment on column jcm_threadmonitorlog.f_threadid is 'thread ID';

comment on column jcm_threadmonitorlog.f_ip is 'machine IP';

comment on column jcm_threadmonitorlog.f_port is 'application port';

comment on column jcm_threadmonitorlog.f_status is 'execution status';

comment on column jcm_threadmonitorlog.f_writedate is 'write time';

comment on column jcm_threadmonitorlog.f_log is 'thread log';

comment on column jcm_threadmonitorlog.f_read is 'read flag';

alter table jcm_threadmonitorlog
    owner to postgres;

create table jcm_threadexceptionlog
(
    f_id         varchar(50)  not null
        primary key,
    f_threadname varchar(255) not null,
    f_flowname   varchar(255),
    f_writetime  timestamp,
    f_message    varchar(4000),
    f_restype    varchar(255),
    f_read       varchar(255),
    f_syscode    varchar(255)
);

comment on table jcm_threadexceptionlog is 'Thread Exception Table';

comment on column jcm_threadexceptionlog.f_id is 'primary key';

comment on column jcm_threadexceptionlog.f_threadname is 'thread name';

comment on column jcm_threadexceptionlog.f_flowname is 'flow name';

comment on column jcm_threadexceptionlog.f_writetime is 'write time';

comment on column jcm_threadexceptionlog.f_message is 'thread log';

comment on column jcm_threadexceptionlog.f_restype is 'resource type';

comment on column jcm_threadexceptionlog.f_read is 'read flag';

comment on column jcm_threadexceptionlog.f_syscode is 'Application system code';

alter table jcm_threadexceptionlog
    owner to postgres;

create table jcm_serverinfolog
(
    f_id          varchar(50) not null
        primary key,
    f_appname     varchar(255),
    f_ip          varchar(255),
    f_port        varchar(255),
    f_cpu         varchar(255),
    f_memory      varchar(255),
    f_hardware    varchar(255),
    f_jvm         varchar(255),
    f_network     varchar(255),
    f_isexception varchar(255),
    f_createtime  timestamp,
    f_updatetime  timestamp,
    f_message     varchar(4000),
    f_apptype     varchar(255),
    f_node        varchar(255),
    f_data        varchar(255)
);

comment on table jcm_serverinfolog is 'Thread Exception Table';

comment on column jcm_serverinfolog.f_id is 'primary key';

comment on column jcm_serverinfolog.f_appname is 'app name';

comment on column jcm_serverinfolog.f_ip is 'ip address';

comment on column jcm_serverinfolog.f_port is 'port';

comment on column jcm_serverinfolog.f_cpu is 'cpu';

comment on column jcm_serverinfolog.f_memory is 'memory ';

comment on column jcm_serverinfolog.f_hardware is 'hardware';

comment on column jcm_serverinfolog.f_jvm is 'jvm';

comment on column jcm_serverinfolog.f_network is 'network';

comment on column jcm_serverinfolog.f_isexception is 'isException';

comment on column jcm_serverinfolog.f_createtime is 'create time';

comment on column jcm_serverinfolog.f_updatetime is 'update time';

comment on column jcm_serverinfolog.f_message is 'thread log';

comment on column jcm_serverinfolog.f_apptype is 'app type';

comment on column jcm_serverinfolog.f_node is 'Node';

comment on column jcm_serverinfolog.f_data is 'Special data';

alter table jcm_serverinfolog
    owner to postgres;

create table jcm_serverexceptioninfolog
(
    f_id         varchar(50) not null
        primary key,
    f_appname    varchar(255),
    f_ip         varchar(255),
    f_port       varchar(255),
    f_createtime timestamp,
    f_message    varchar(4000),
    f_read       varchar(255)
);

comment on table jcm_serverexceptioninfolog is 'Server Exception Log Table';

comment on column jcm_serverexceptioninfolog.f_id is 'primary key';

comment on column jcm_serverexceptioninfolog.f_appname is 'app name';

comment on column jcm_serverexceptioninfolog.f_ip is 'ip address';

comment on column jcm_serverexceptioninfolog.f_port is 'port';

comment on column jcm_serverexceptioninfolog.f_createtime is 'create time';

comment on column jcm_serverexceptioninfolog.f_message is 'thread log';

comment on column jcm_serverexceptioninfolog.f_read is 'read flag';

alter table jcm_serverexceptioninfolog
    owner to postgres;

create table jcm_proxy_error
(
    f_id             varchar(50) not null
        primary key,
    f_agentname      varchar(255),
    f_occurrencetime timestamp
);

comment on table jcm_proxy_error is 'Acting stacking machine record sheet';

comment on column jcm_proxy_error.f_id is 'primary key';

comment on column jcm_proxy_error.f_agentname is 'proxy name';

comment on column jcm_proxy_error.f_occurrencetime is 'Agent failure time';

alter table jcm_proxy_error
    owner to postgres;

create table jcm_jobflowlogsteps
(
    f_id            varchar(50) not null
        primary key,
    f_jfinstancedid varchar(50),
    f_jobid         varchar(50),
    f_jobflowid     varchar(50),
    f_level         varchar(30),
    f_position      varchar(100),
    f_serve         varchar(100),
    f_step          varchar(100),
    f_message       varchar(4000),
    f_writetime     timestamp
);

comment on table jcm_jobflowlogsteps is 'Job flow execution log table';

comment on column jcm_jobflowlogsteps.f_id is 'Primary key';

comment on column jcm_jobflowlogsteps.f_jfinstancedid is 'Job stream instance ID';

comment on column jcm_jobflowlogsteps.f_jobid is 'Job id';

comment on column jcm_jobflowlogsteps.f_jobflowid is 'Job flow id';

comment on column jcm_jobflowlogsteps.f_level is 'log leve';

comment on column jcm_jobflowlogsteps.f_position is 'Log position';

comment on column jcm_jobflowlogsteps.f_serve is 'server';

comment on column jcm_jobflowlogsteps.f_step is 'Step';

comment on column jcm_jobflowlogsteps.f_message is 'Log message';

comment on column jcm_jobflowlogsteps.f_writetime is 'Log write time';

alter table jcm_jobflowlogsteps
    owner to postgres;

create index ix_flowlog_instanceid
    on jcm_jobflowlogsteps (f_jfinstancedid);

create table job_sche_hourly
(
    src_name       varchar(10),
    table_name     varchar(100),
    batch_dt       varchar(20),
    ext_start_time varchar(20),
    prog_end_time  varchar(20),
    ext_status     varchar(10)
);

alter table job_sche_hourly
    owner to postgres;

create table job_sche_5minutes
(
    src_name       varchar(10),
    table_name     varchar(100),
    batch_dt       varchar(20),
    ext_start_time varchar(20),
    prog_end_time  varchar(20),
    ext_status     varchar(10)
);

alter table job_sche_5minutes
    owner to postgres;

