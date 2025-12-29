classdef DatabaseConnector
    properties
        host
        host2
        dbname
        username
        password
        % 可配置项：默认score来源和本地预测文件夹
        score_source
        local_score_dir
    end

    methods
        function obj = DatabaseConnector()
            config_db;
            obj.host = db_config.host;
            obj.host2 = db_config.host2;
            obj.dbname = db_config.dbname;
            obj.username = db_config.username;
            obj.password = db_config.password;
            obj.score_source = db_config.score_source;
            obj.local_score_dir = db_config.local_score_dir;
            
        end

        function conn = createConnectionWithHost(obj, hostValue)
            % Create a JDBC connection using a specific host string (e.g. obj.host2)
            if isempty(hostValue)
                error('createConnectionWithHost:EmptyHost', 'hostValue is empty.');
            end

            [hostName, portNumber] = obj.parseHost(hostValue);
            jdbcUrl = sprintf('jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=FALSE', ...
                              hostName, portNumber, obj.dbname);
            jdbcUrl = sprintf('%s&connectTimeout=5000&socketTimeout=20000', jdbcUrl);

            conn = database('', obj.username, obj.password, ...
                            'com.mysql.cj.jdbc.Driver', jdbcUrl);
            if ~isopen(conn)
                details = conn.Message;
                if isempty(details)
                    details = '未知原因';
                elseif contains(details, 'Communications link failure')
                    details = sprintf('无法与 %s:%d 建立连接，请检查网络连通性、VPN 或防火墙设置。原始信息: %s', ...
                                      hostName, portNumber, details);
                end
                error('DatabaseConnector:ConnectionFailed', '数据库连接失败: %s', details);
            end
        end
        
        function conn = openConnection(obj)
            % Provide a public accessor for the underlying MySQL connection
            conn = obj.createConnection();
            if ~isopen(conn)
                details = conn.Message;
                if isempty(details)
                    details = '未知原因';
                end
                error('DatabaseConnector:ConnectionFailed', '数据库连接失败: %s', details);
            end
        end
        
        
        function actual_date_str = convertAndValidateDate(~, date)
            %CONVERTANDVALIDATEDATE 转换日期格式并验证交易日
            %   统一处理日期转换和交易日验证，减少重复代码
            %   输入: date - 原始日期 (datetime, char, string)
            %   输出: actual_date_str - 实际使用的交易日 (string, 格式: yyyy-MM-dd)
            
            [~, actual_date_str] = ValidateWorkingDay(date);
        end

        function [df_result] = index_component_withdraw(obj, date, index_type)
            
            % 先验证交易日，如果不是交易日则使用最近的前一个交易日
            [~, actual_date_str] = ValidateWorkingDay(date);
            
            conn = obj.createConnection();
            
            
            % 根据指数类型查询对应的成分股
            if strcmp(index_type, '沪深300') || strcmp(index_type, 'hs300')
                organization = 'hs300';
            elseif strcmp(index_type, 'zz500') || strcmp(index_type, '中证500')
                organization = 'zz500';
            elseif strcmp(index_type, 'zz1000') || strcmp(index_type, '中证1000')
                organization = 'zz1000';
            elseif strcmp(index_type, 'zz2000') || strcmp(index_type, '中证2000')
                organization = 'zz2000';
            elseif strcmp(index_type, 'zzA500') || strcmp(index_type, '中证A500')
                organization = 'zzA500';
            else
                % 默认使用沪深300
                organization = 'hs300';
            end
            
            
            try
                % 查询指定指数的成分股（使用已验证的交易日）
                query = ['SELECT code, weight FROM data_indexcomponent ', ...
                              'WHERE organization = ''', organization, ''' ', ...
                              'AND valuation_date = ''', actual_date_str, ''''];
                
                df_result = fetch(conn, query);
                 
                % 如果没有找到数据，尝试查找最近的日期
                if isempty(df_result)
                    fallback_query = ['SELECT valuation_date FROM data_indexcomponent ', ...
                                     'WHERE organization = ''', organization, ''' ', ...
                                     'AND valuation_date <= ''', actual_date_str, ''' ', ...
                                     'ORDER BY valuation_date DESC LIMIT 1'];
                    
                    fallback_result = fetch(conn, fallback_query);
                    if ~isempty(fallback_result)
                        % 使用找到的最近日期重新查询
                        fallback_date = char(fallback_result.valuation_date(1));
                        fprintf_log('INFO: 未找到 %s 的数据，使用最近日期: %s\n', actual_date_str, fallback_date);
                        
                        fallback_data_query = ['SELECT code, weight FROM data_indexcomponent ', ...
                                              'WHERE organization = ''', organization, ''' ', ...
                                              'AND valuation_date = ''', fallback_date, ''''];
                        df_result = fetch(conn, fallback_data_query);
                    else
                        fprintf_log('WARNING: 未找到 %s 及之前的任何数据\n', actual_date_str);
                    end
                end
                
            catch ME
                % close(conn);
                rethrow(ME);
            end
            close(conn);
        end

        function [df_hs300, df_zz500, df_zz1000, df_zz2000,df_zzA500] = index_exposure_withdraw(obj, date)

            conn = obj.createConnection();
            if isdatetime(date)
                date_str = char(string(date, 'yyyy-MM-dd'));
            elseif ischar(date) || isstring(date)
                % 输入是 string/char，自动判断格式
                if ~isempty(regexp(date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                    % yyyy-MM-dd
                    dt = datetime(date, 'InputFormat', 'yyyy-MM-dd');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                elseif ~isempty(regexp(date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                    % dd-MMM-yyyy
                    dt = datetime(date, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                else
                    date_str = char(date);
                end
            end
            
            try
                % 使用与stock_factor_exposure_withdraw相同的策略
                % 首先获取列名
                meta_query = 'SHOW COLUMNS FROM data_factorindexexposure';
                meta_result = fetch(conn, meta_query);
                
                if ~isempty(meta_result)
                    % 获取数据库中的实际列名
                    if ismember('Field', meta_result.Properties.VariableNames)
                        db_column_names = meta_result.Field;
                    else
                        db_column_names = meta_result{:, 1};
                    end
                    
                    % 过滤掉不需要的列（指数数据不需要code列）
                    excluded_columns = {'valuation_date', 'update_time', 'organization', 'code'};
                    factor_columns = db_column_names(~ismember(db_column_names, excluded_columns));
                    
                    
                    % 构建精确的查询，使用别名来避免列名问题
                    select_parts = {};
                    for i = 1:length(factor_columns)
                        col_name = factor_columns{i};
                        % 为每个列创建别名，避免MATLAB的列名问题
                        alias = sprintf('col_%d', i);
                        select_parts{end+1} = sprintf('%s AS %s', col_name, alias);
                    end
                    
                    % 构建完整的查询
                    query6 = ['SELECT ', strjoin(select_parts, ', '), ...
                              ' FROM data_factorindexexposure ', ...
                              'WHERE organization = ''hs300'' ', ...
                              'AND valuation_date = ''', date_str, ''''];
                    
                    
                    df_hs300 = fetch(conn, query6);
                    
                    % 将别名替换为实际的列名
                    if ~isempty(df_hs300)
                        % 直接使用数据库中的列名
                        df_hs300.Properties.VariableNames = factor_columns;
                    end
                    
                   
                else
                    error('无法获取数据库列信息');
                end
                
                % 查询中证500因子暴露数据
                query7 = ['SELECT ', strjoin(select_parts, ', '), ...
                          ' FROM data_factorindexexposure ', ...
                          'WHERE organization = ''zz500'' ', ...
                          'AND valuation_date = ''', date_str, ''''];
                df_zz500 = fetch(conn, query7);
                if ~isempty(df_zz500)
                    df_zz500.Properties.VariableNames = factor_columns;
                end
                
                % 查询中证1000因子暴露数据
                query8 = ['SELECT ', strjoin(select_parts, ', '), ...
                          ' FROM data_factorindexexposure ', ...
                          'WHERE organization = ''zz1000'' ', ...
                          'AND valuation_date = ''', date_str, ''''];
                df_zz1000 = fetch(conn, query8);
                if ~isempty(df_zz1000)
                    df_zz1000.Properties.VariableNames = factor_columns;
                end
                
                % 查询中证2000因子暴露数据
                query9 = ['SELECT ', strjoin(select_parts, ', '), ...
                          ' FROM data_factorindexexposure ', ...
                          'WHERE organization = ''zz2000'' ', ...
                          'AND valuation_date = ''', date_str, ''''];
                df_zz2000 = fetch(conn, query9);
                if ~isempty(df_zz2000)
                    df_zz2000.Properties.VariableNames = factor_columns;
                end
                
                % 查询中证A500因子暴露数据
                query10 = ['SELECT ', strjoin(select_parts, ', '), ...
                           ' FROM data_factorindexexposure ', ...
                           'WHERE organization = ''zzA500'' ', ...
                           'AND valuation_date = ''', date_str, ''''];
                df_zzA500 = fetch(conn, query10);
                if ~isempty(df_zzA500)
                    df_zzA500.Properties.VariableNames = factor_columns;
                end
                
            catch ME
                close(conn);
                rethrow(ME);
            end
            close(conn);
        end

        function [df_stockpool] = stock_pool_withdraw(obj, date)
            conn = obj.createConnection();
            
            if isdatetime(date)
                date_str = char(string(date, 'yyyy-MM-dd'));
            elseif ischar(date) || isstring(date)
                % 输入是 string/char，自动判断格式
                if ~isempty(regexp(date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                    % yyyy-MM-dd
                    dt = datetime(date, 'InputFormat', 'yyyy-MM-dd');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                elseif ~isempty(regexp(date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                    % dd-MMM-yyyy
                    dt = datetime(date, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                else
                    date_str = char(date);
                end
            end
            
            try
               
                query11 = ['SELECT valuation_date, code FROM data_prepared_new.data_factorpool ', ...
                               'WHERE valuation_date = ''', date_str, ''''];
                df_stockpool = fetch(conn, query11);
            catch ME
                % close(conn);
                rethrow(ME);
            end
            close(conn);
  
        end

        function [df_factor] = stock_factor_exposure_withdraw(obj, date)
            conn = obj.createConnection();
            
            if isdatetime(date)
                date_str = char(string(date, 'yyyy-MM-dd'));  
            elseif ischar(date) || isstring(date)
                % 输入是 string/char，自动判断格式
                if ~isempty(regexp(date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                    % yyyy-MM-dd
                    dt = datetime(date, 'InputFormat', 'yyyy-MM-dd');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                elseif ~isempty(regexp(date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                    % dd-MMM-yyyy
                    dt = datetime(date, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                else
                    date_str = char(date);
                end
            end
            
            try
             
                % 首先获取列名
                meta_query = 'SHOW COLUMNS FROM data_prepared_new.data_factorexposure';
                meta_result = fetch(conn, meta_query);
                
                if ~isempty(meta_result)
                    % 获取数据库中的实际列名
                    if ismember('Field', meta_result.Properties.VariableNames)
                        db_column_names = meta_result.Field;
                    else
                        db_column_names = meta_result{:, 1};
                    end
                    
                    % 过滤掉不需要的列和缺少的行业因子
                    excluded_columns = {'valuation_date', 'update_time', 'organization', '电力设备', '电子元器件', '餐饮旅游'};
                    factor_columns = db_column_names(~ismember(db_column_names, excluded_columns));
                    
                   
                    % 构建精确的查询，使用别名来避免列名问题
                    select_parts = {};
                    for i = 1:length(factor_columns)
                        col_name = factor_columns{i};
                        % 为每个列创建别名，避免MATLAB的列名问题
                        alias = sprintf('col_%d', i);
                        select_parts{end+1} = sprintf('%s AS %s', col_name, alias);
                    end
                    
                    % 构建完整的查询
                    query12 = ['SELECT ', strjoin(select_parts, ', '), ...
                              ' FROM data_prepared_new.data_factorexposure ', ...
                              'WHERE valuation_date = ''', date_str, ''''];
                    
                  
                    df_factor = fetch(conn, query12);
                    
                    % 将别名替换为实际的列名
                    if ~isempty(df_factor)
                        % 直接使用数据库中的列名
                        df_factor.Properties.VariableNames = factor_columns;
                    end
                    
                   
                else
                    error('无法获取数据库列信息');
                end
                
            catch ME
                % close(conn);
                rethrow(ME);
            end
            % close(conn);
  
        end

        function [df] = factor_cov_withdraw(obj, date)
            conn = obj.createConnection();
            
            if isdatetime(date)
                date_str = char(string(date, 'yyyy-MM-dd'));  
            elseif ischar(date) || isstring(date)
                % 输入是 string/char，自动判断格式
                if ~isempty(regexp(date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                    % yyyy-MM-dd
                    dt = datetime(date, 'InputFormat', 'yyyy-MM-dd');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                elseif ~isempty(regexp(date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                    % dd-MMM-yyyy
                    dt = datetime(date, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                else
                    date_str = char(date);
                end
            end
            
            try
                % 使用更精确的查询，避免MATLAB列名问题
                % 首先获取列名
                meta_query = 'SHOW COLUMNS FROM data_prepared_new.data_factorcov';
                meta_result = fetch(conn, meta_query);
                
                if ~isempty(meta_result)
                    % 获取数据库中的实际列名
                    if ismember('Field', meta_result.Properties.VariableNames)
                        db_column_names = meta_result.Field;
                    else
                        db_column_names = meta_result{:, 1};
                    end
                    
                    % 过滤掉不需要的列和缺少的行业因子
                    excluded_columns = {'valuation_date', 'update_time', 'organization', '电力设备', '电子元器件', '餐饮旅游'};
                    factor_columns = db_column_names(~ismember(db_column_names, excluded_columns));
                    
                    
                    % 构建精确的查询，使用别名来避免列名问题
                    select_parts = {};
                    for i = 1:length(factor_columns)
                        col_name = factor_columns{i};
                        % 为每个列创建别名，避免MATLAB的列名问题
                        alias = sprintf('col_%d', i);
                        select_parts{end+1} = sprintf('%s AS %s', col_name, alias);
                    end
                    
                    % 构建完整的查询
                    query13 = ['SELECT ', strjoin(select_parts, ', '), ...
                              ' FROM data_prepared_new.data_factorcov ', ...
                              'WHERE valuation_date = ''', date_str, ''''];
                    
             
                    df = fetch(conn, query13);
                    
                    % 将别名替换为实际的列名
                    if ~isempty(df)
                        % 直接使用数据库中的列名
                        df.Properties.VariableNames = factor_columns;
                    end
                
                else
                    error('无法获取数据库列信息');
                end
             
            catch ME
                % close(conn);
                rethrow(ME);
            end
            % close(conn);
        end

        function [df] = factor_risk_withdraw(obj, date)
            conn = obj.createConnection();
            
            if isdatetime(date)
                date_str = char(string(date, 'yyyy-MM-dd'));  
            elseif ischar(date) || isstring(date)
                % 输入是 string/char，自动判断格式
                if ~isempty(regexp(date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                    % yyyy-MM-dd
                    dt = datetime(date, 'InputFormat', 'yyyy-MM-dd');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                elseif ~isempty(regexp(date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                    % dd-MMM-yyyy
                    dt = datetime(date, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                else
                    date_str = char(date);
                end
            end
            
            try
               
                query14 = ['SELECT code, specificrisk FROM data_prepared_new.data_factorspecificrisk ', ...
                               'WHERE valuation_date = ''', date_str, ''''];
                df = fetch(conn, query14);
                if ismember('update_time', df.Properties.VariableNames)
                    df = removevars(df, 'update_time');
                end
                
                
                % 确保code列是字符类型
                if ~iscellstr(df.code) && ~isstring(df.code)
                    df.code = cellstr(string(df.code));
                end
                
                % 确保specificrisk列是数值类型
                if ~isnumeric(df.specificrisk)
                    df.specificrisk = str2double(string(df.specificrisk));
                end
                
                % 创建转置表：行是风险因子，列是股票代码
                % 获取唯一的股票代码
                unique_codes = unique(df.code);
                
                % 创建新的表结构，先创建空的数值数组
                risk_values = zeros(1, length(unique_codes));
                
                % 为每个股票代码获取风险值
                for i = 1:length(unique_codes)
                    code = unique_codes{i};
                    % 找到该股票的风险值
                    idx = strcmp(df.code, code);
                    if any(idx)
                        risk_value = df.specificrisk(idx);
                        % 如果有多行，取第一个值
                        if length(risk_value) > 1
                            risk_value = risk_value(1);
                        end
                        risk_values(i) = risk_value;
                    end
                end
                
                % 创建最终的表，使用正确的列名
                df = array2table(risk_values, 'VariableNames', unique_codes);
                df.Properties.RowNames = {'specificrisk'};
                
                
                
             
            catch ME
                % close(conn);
                rethrow(ME);
            end
            % close(conn);
        end

        function [df] = score_withdraw(obj, date, score_name, source)
           

            if nargin < 4 || isempty(source)
                source = obj.score_source;
            end

            if ~any(strcmpi(source, {'db','csv'}))
                error('score_withdraw: invalid source. source must be ''db'' or ''csv''');
            end

            % 规范化日期字符串
            if isdatetime(date)
                date_str = char(string(date, 'yyyy-MM-dd'));  
            elseif ischar(date) || isstring(date)
                if ~isempty(regexp(date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                    dt = datetime(date, 'InputFormat', 'yyyy-MM-dd');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                elseif ~isempty(regexp(date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                    dt = datetime(date, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
                    date_str = char(string(dt, 'yyyy-MM-dd'));
                else
                    date_str = char(date);
                end
            else
                date_str = char(string(date));
            end

            df = table();

            % 如果 source 为 'db'，从数据库读取
            if strcmpi(source, 'db') & strcmpi(score_name, 'vp08')
                if isprop(obj, 'host2') && ~isempty(obj.host2)
                    try
                        conn = obj.createConnectionWithHost(obj.host2);
                    catch connErr
                        
                        rethrow(connErr);
                    end
                else
                    conn = obj.createConnection();
                end
                try
                    query15 = ['SELECT * FROM data_prepared_new.data_score_dev ', ...
                                   'WHERE valuation_date = ''', date_str, '''', ...
                                   'AND score_name = ''', score_name, ''''];
                    df_db = fetch(conn, query15);
                    if ismember('update_time', df_db.Properties.VariableNames)
                        df_db = removevars(df_db, 'update_time');
                    end
                catch ME
                    % close(conn);
                    rethrow(ME);
                end
                close(conn);
                if ~isempty(df_db)
                    df = df_db;
                    return;
                end
            end

            if strcmpi(source, 'db')
                conn = obj.createConnection();
                try
                    query16 = ['SELECT * FROM data_prepared_new.data_score ', ...
                                   'WHERE valuation_date = ''', date_str, '''', ...
                                   'AND score_name = ''', score_name, ''''];
                    df_db = fetch(conn, query16);
                    if ismember('update_time', df_db.Properties.VariableNames)
                        df_db = removevars(df_db, 'update_time');
                    end
                catch ME
                    % close(conn);
                    rethrow(ME);
                end
                close(conn);
                if ~isempty(df_db)
                    df = df_db;
                    return;
                end
            end

            % 如果到这里且 source 指定为 'csv'，则尝试从本地 prediction CSV 读取
            if strcmpi(source, 'csv')
                try
                    % 决定搜索目录
                    if ~isempty(obj.local_score_dir)
                        search_root = obj.local_score_dir;
                    else
                        search_root = pwd;
                    end

                    try
                        date_ymd = datestr(date, 'yyyymmdd');
                    catch
                        date_ymd = regexprep(date_str, '-', '');
                    end
                    pattern1 = fullfile(search_root, '**', ['prediction_*' date_ymd '*.csv']);
                    files = dir(pattern1);
                    if isempty(files)
                        pattern2 = fullfile(search_root, '**', ['prediction_*' date_str '*.csv']);
                        files = dir(pattern2);
                    end

                    if ~isempty(files)
                        pred_path = fullfile(files(1).folder, files(1).name);
                        try
                            df_file = readtable(pred_path);
                        catch
                            df_file = readtable(pred_path, 'ReadVariableNames', false);
                        end

                        varnames = df_file.Properties.VariableNames;
                        if isempty(varnames) || (length(varnames) >= 2 && startsWith(varnames{1}, 'Var'))
                            df_file.Properties.VariableNames = {'code', 'score'};
                            varnames = df_file.Properties.VariableNames;
                        end

                        if any(strcmpi(varnames, 'score')) && ~any(strcmpi(varnames, 'final_score'))
                            idx = find(strcmpi(varnames, 'score'), 1);
                            df_file.Properties.VariableNames{idx} = 'final_score';
                            varnames = df_file.Properties.VariableNames;
                        end

                        if ~any(strcmpi(varnames, 'code')) || ~any(strcmpi(varnames, 'final_score'))
                            error('本地 prediction 文件不包含必须的列: code 和 score/final_score');
                        end

                        if ~any(strcmpi(varnames, 'score_name'))
                            df_file.score_name = repmat({char(score_name)}, height(df_file), 1);
                        end

                        df = df_file(:, intersect({'code','final_score','score_name'}, df_file.Properties.VariableNames, 'stable'));
                        return;
                    else
                        % 未找到本地文件，返回空表（或保持 df_from_db 如果存在）
                        if isempty(df)
                            fprintf_log('WARN: 未找到匹配的 prediction CSV 文件: %s\n', search_root);
                        end
                        return;
                    end
                catch MEfile
                    fprintf_log('WARN: 从本地 prediction 文件读取失败: %s\n', MEfile.message);
                    return;
                end
            end
        end


    end

    methods (Access = private)
        function conn = createConnection(obj)
            [hostName, portNumber] = obj.parseHost(obj.host);
            jdbcUrl = sprintf('jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=FALSE', ...
                              hostName, portNumber, obj.dbname);
            jdbcUrl = sprintf('%s&connectTimeout=5000&socketTimeout=20000', jdbcUrl);

            conn = database('', obj.username, obj.password, ...
                            'com.mysql.cj.jdbc.Driver', jdbcUrl);
            if ~isopen(conn)
                details = conn.Message;
                if isempty(details)
                    details = '未知原因';
                elseif contains(details, 'Communications link failure')
                    details = sprintf('无法与 %s:%d 建立连接，请检查网络连通性、VPN 或防火墙设置。原始信息: %s', ...
                                      hostName, portNumber, details);
                end
                error('DatabaseConnector:ConnectionFailed', '数据库连接失败: %s', details);
            end
        end

        function [hostName, portNumber] = parseHost(~, hostValue)
            % Extract hostname and port from configuration string, defaulting to MySQL 3306
            defaultPort = 3306;
            parts = strsplit(strtrim(hostValue), ':');
            hostName = parts{1};
            if numel(parts) >= 2
                portNumber = str2double(parts{2});
                if isnan(portNumber)
                    portNumber = defaultPort;
                end
            else
                portNumber = defaultPort;
            end
        end

        function fixed_df = fix_matlab_column_names(obj, df)
            % 修复MATLAB自动生成的列名问题
            
            % 删除不需要的列
            columns_to_remove = {'update_time', 'valuation_date', 'organization'};
            existing_remove_cols = {};
            for i = 1:length(columns_to_remove)
                if ismember(columns_to_remove{i}, df.Properties.VariableNames)
                    existing_remove_cols{end+1} = columns_to_remove{i};
                end
            end
            
            if ~isempty(existing_remove_cols)
                df = removevars(df, existing_remove_cols);
            end
            
            % 获取当前列名
            current_cols = df.Properties.VariableNames;
            
            % 识别异常的列名
            problematic_indices = [];
            for i = 1:length(current_cols)
                col_name = current_cols{i};
                % 检测异常列名：以x开头且包含下划线，或包含多个下划线
                if (startsWith(col_name, 'x') && contains(col_name, '_')) || ...
                   (contains(col_name, '____') || contains(col_name, '___')) || ...
                   (contains(col_name, '_____') || contains(col_name, '______'))
                    problematic_indices(end+1) = i;
                end
            end
            
            if ~isempty(problematic_indices)
                fprintf_log('发现 %d 个异常列名，正在修复...\n', length(problematic_indices));
                
                % 尝试从数据库获取正确的列名
                try
                    conn = obj.createConnection();
                    meta_query = 'SHOW COLUMNS FROM data_prepared_new.data_factorexposure';
                    meta_result = fetch(conn, meta_query);
                    close(conn);
                    
                    if ~isempty(meta_result)
                        % 获取数据库中的实际列名
                        if ismember('Field', meta_result.Properties.VariableNames)
                            db_column_names = meta_result.Field;
                        else
                            db_column_names = meta_result{:, 1};
                        end
                        
                        % 过滤掉不需要的列，保持原始顺序
                        db_column_names = db_column_names(~ismember(db_column_names, {'valuation_date', 'update_time', 'organization'}));
                        
                        fprintf_log('数据库中的列名 (共%d个): %s\n', length(db_column_names), strjoin(db_column_names, ', '));
                        
                        % 创建修复后的列名数组
                        fixed_cols = current_cols;
                        
                        % 为异常列名分配数据库中的实际名称
                        % 注意：需要根据异常列名的位置来分配正确的数据库列名
                        for i = 1:length(problematic_indices)
                            idx = problematic_indices(i);
                            old_name = current_cols{idx};
                            
                            % 计算这个异常列名在数据库列名中的对应位置
                            % 假设异常列名是按照数据库列的顺序出现的
                            db_idx = idx;
                            
                            % 如果数据库列名数量足够
                            if db_idx <= length(db_column_names)
                                new_name = db_column_names{db_idx};
                                
                                % 检查新名称是否已经存在
                                if ~ismember(new_name, fixed_cols)
                                    fixed_cols{idx} = new_name;
                                    fprintf_log('  修复列名: %s -> %s\n', old_name, new_name);
                                else
                                    % 如果名称已存在，添加后缀
                                    counter = 1;
                                    unique_name = sprintf('%s_%d', new_name, counter);
                                    while ismember(unique_name, fixed_cols)
                                        counter = counter + 1;
                                        unique_name = sprintf('%s_%d', new_name, counter);
                                    end
                                    fixed_cols{idx} = unique_name;
                                    fprintf_log('  修复列名: %s -> %s\n', old_name, unique_name);
                                end
                            else
                                % 如果超出数据库列名范围，使用默认名称
                                new_name = sprintf('factor_%d', i);
                                fixed_cols{idx} = new_name;
                                    fprintf_log('  修复列名: %s -> %s (默认名称)\n', old_name, new_name);
                            end
                        end
                        
                        % 应用修复后的列名
                        df.Properties.VariableNames = fixed_cols;
                        fprintf_log('列名修复完成\n');
                        
                    else
                        error('无法获取数据库列名');
                    end
                    
                catch ME
                    fprintf_log('无法从数据库获取列名，跳过修复: %s\n', ME.message);
                    % 如果无法修复，保持原始列名
                end
            end
            
            fixed_df = df;
        end
    end
end