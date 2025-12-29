function write_portfolio_info_to_db(portfolio_info)
% write_portfolio_info_to_db - 将 portfolio_info 写入数据库
%
% 输入:
%   portfolio_info - 投资组合信息表

    currentDir = fileparts(mfilename('fullpath'));
    db_config = fullfile(currentDir, '..', '..', 'config', 'db.yaml');
    db_info = ReadYaml(db_config);
    
    host = db_info.host2;
    dbname = db_info.database5;
    username = db_info.user;
    password = db_info.password;
    table_name = db_info.table_name4;
    
    % 创建数据库连接
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);
    
    if ~isopen(conn)
        error('数据库连接失败: %s', conn.Message);
    end
    
    try
        % 数据库表的固定列名
        % 主键: valuation_date, score_name
        db_col_names = {'valuation_date', 'mode_type', 'score_name', 'index_type', 'base_score', 'update_time'};
        col_list = strjoin(db_col_names, ', ');
        
   
        non_key_cols = {'mode_type', 'index_type', 'base_score', 'update_time'};
        update_clause = cell(length(non_key_cols), 1);
        for i = 1:length(non_key_cols)
            update_clause{i} = sprintf('%s=VALUES(%s)', non_key_cols{i}, non_key_cols{i});
        end
        update_str = strjoin(update_clause, ', ');
        
        % 获取当前时间
        current_time = datestr(now, 'yyyy-mm-dd HH:MM:SS');
        
        % 添加 WorkingDaysList 路径
        addpath(fullfile(currentDir, '..', 'tools'));
        
        % 检查必需的列是否存在
        required_cols = {'start_date', 'end_date', 'mode_type', 'portfolio_name', 'index_type', 'score_type'};
        missing_cols = {};
        for j = 1:length(required_cols)
            if ~ismember(required_cols{j}, portfolio_info.Properties.VariableNames)
                missing_cols{end+1} = required_cols{j};
            end
        end
        
        % if ~isempty(missing_cols)
        %     warning('portfolio_info 表中缺少必需的列: %s，跳过写入数据库', strjoin(missing_cols, ', '));
        %     return;
        % end
        
        total_records = 0;
        
        % 逐行处理每个投资组合
        for i = 1:height(portfolio_info)
            % 获取 start_date 和 end_date
            start_date = portfolio_info.start_date(i);
            end_date = portfolio_info.end_date(i);
            
            % 转换日期格式
            if isdatetime(start_date)
                start_date_str = datestr(start_date, 'yyyy-mm-dd');
            elseif iscell(start_date)
                if ischar(start_date{1})
                    start_date_str = start_date{1};
                elseif isdatetime(start_date{1})
                    start_date_str = datestr(start_date{1}, 'yyyy-mm-dd');
                else
                    start_date_str = char(string(start_date{1}));
                end
            elseif isstring(start_date) || ischar(start_date)
                start_date_str = char(start_date);
                if contains(start_date_str, '/')
                    start_date_str = strrep(start_date_str, '/', '-');
                end
            else
                try
                    start_date_str = datestr(start_date, 'yyyy-mm-dd');
                catch
                    start_date_str = char(string(start_date));
                end
            end
            
            if isdatetime(end_date)
                end_date_str = datestr(end_date, 'yyyy-mm-dd');
            elseif iscell(end_date)
                if ischar(end_date{1})
                    end_date_str = end_date{1};
                elseif isdatetime(end_date{1})
                    end_date_str = datestr(end_date{1}, 'yyyy-mm-dd');
                else
                    end_date_str = char(string(end_date{1}));
                end
            elseif isstring(end_date) || ischar(end_date)
                end_date_str = char(end_date);
                if contains(end_date_str, '/')
                    end_date_str = strrep(end_date_str, '/', '-');
                end
            else
                try
                    end_date_str = datestr(end_date, 'yyyy-mm-dd');
                catch
                    end_date_str = char(string(end_date));
                end
            end
            
            % 获取该投资组合的所有工作日
            try
                workday_table = WorkingDaysList(start_date_str, end_date_str);
                if isempty(workday_table) || height(workday_table) == 0
                    warning('投资组合 %d 在日期范围 %s 到 %s 内没有工作日，跳过', i, start_date_str, end_date_str);
                    continue;
                end
                workday_list = workday_table{:, 1};
                
                % 转换为字符串格式
                if isdatetime(workday_list)
                    workday_str_list = cellstr(datestr(workday_list, 'yyyy-mm-dd'));
                elseif iscell(workday_list)
                    workday_str_list = cellfun(@(x) char(string(x)), workday_list, 'UniformOutput', false);
                else
                    workday_str_list = cellstr(string(workday_list));
                end
            catch ME
                warning('获取投资组合 %d 的工作日列表失败: %s，跳过', i, ME.message);
                continue;
            end
            
            % 提取投资组合的其他信息
            % mode_type
            mode_type_val = portfolio_info.mode_type(i);
            if iscell(mode_type_val) && ischar(mode_type_val{1})
                mode_type_str = mode_type_val{1};
            elseif isstring(mode_type_val) || ischar(mode_type_val)
                mode_type_str = char(mode_type_val);
            else
                mode_type_str = char(string(mode_type_val));
            end
            
            % score_name (portfolio_name)
            score_name_val = portfolio_info.portfolio_name(i);
            if iscell(score_name_val) && ischar(score_name_val{1})
                score_name_str = score_name_val{1};
            elseif isstring(score_name_val) || ischar(score_name_val)
                score_name_str = char(score_name_val);
            else
                score_name_str = char(string(score_name_val));
            end
            
            % index_type
            index_type_val = portfolio_info.index_type(i);
            if iscell(index_type_val) && ischar(index_type_val{1})
                index_type_str = index_type_val{1};
            elseif isstring(index_type_val) || ischar(index_type_val)
                index_type_str = char(index_type_val);
            else
                index_type_str = char(string(index_type_val));
            end
            
            % base_score (score_type)
            base_score_val = portfolio_info.score_type(i);
            if iscell(base_score_val) && ischar(base_score_val{1})
                base_score_str = base_score_val{1};
            elseif isstring(base_score_val) || ischar(base_score_val)
                base_score_str = char(base_score_val);
            else
                base_score_str = char(string(base_score_val));
            end
            
            % 转义 SQL 特殊字符
            mode_type_str = strrep(mode_type_str, '''', '''''');
            score_name_str = strrep(score_name_str, '''', '''''');
            index_type_str = strrep(index_type_str, '''', '''''');
            base_score_str = strrep(base_score_str, '''', '''''');
            
            % 为每个工作日创建一条记录
            for j = 1:length(workday_str_list)
                valuation_date_str = workday_str_list{j};
                
                % 构建 VALUES 字符串
                values_str = sprintf('''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s''', ...
                                    valuation_date_str, mode_type_str, score_name_str, ...
                                    index_type_str, base_score_str, current_time);
                
                % 构建完整的 INSERT 语句
                insert_sql = sprintf('INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s', ...
                                    table_name, col_list, values_str, update_str);
                
                % 执行插入
                exec(conn, insert_sql);
                total_records = total_records + 1;
            end
        end
        
        fprintf('成功写入 %d 条 portfolio_info 记录到数据库表 %s\n', ...
                total_records, table_name);
        
    catch ME
        close(conn);
        rethrow(ME);
    end
    
    close(conn);
end

