function [df_index_return] = index_return_withdraw(start_date, end_date)
    %INDEX_RETURN_WITHDRAW 提取指数收益率数据
    %   从数据库中提取指定日期范围内的指数收益率数据
    
    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);

    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);

    % 兼容不同MATLAB版本的日期格式化
    if isdatetime(start_date)
        start_date_str = datestr(start_date, 'yyyy-mm-dd');
    elseif ischar(start_date) || isstring(start_date)
        date_str = char(start_date);
        % 检测日期格式并转换
        if contains(date_str, '-') && length(date_str) == 11
            % dd-MMM-yyyy format
            dt = datetime(date_str, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
            start_date_str = datestr(dt, 'yyyy-mm-dd');
            
        else
            % 假设是 yyyy-mm-dd 格式
            start_date_str = date_str;
        end
    end
    
    if isdatetime(end_date)
        end_date_str = datestr(end_date, 'yyyy-mm-dd');
    elseif ischar(end_date) || isstring(end_date)
        date_str = char(end_date);
        % 检测日期格式并转换
        if contains(date_str, '-') && length(date_str) == 11
            % dd-MMM-yyyy format
            dt = datetime(date_str, 'InputFormat', 'dd-MMM-yyyy', 'Locale','en_US');
            end_date_str = datestr(dt, 'yyyy-mm-dd');
        else
            % 假设是 yyyy-mm-dd 格式
            end_date_str = date_str;
        end
    end

    

    query = sprintf('SELECT valuation_date, code, pct_chg FROM data_prepared_new.data_index WHERE valuation_date BETWEEN ''%s'' AND ''%s''', start_date_str, end_date_str);
    
    df_index_return = fetch(conn, query);
    
    % 确保数据类型正确
    if ~isempty(df_index_return)
        % 确保valuation_date是字符串类型
        if iscell(df_index_return.valuation_date)
            df_index_return.valuation_date = string(df_index_return.valuation_date);
        end
        
        % 确保code是字符串类型
        if iscell(df_index_return.code)
            df_index_return.code = string(df_index_return.code);
        end
    else
        fprintf(' 未获取到任何指数收益率数据\n');
    end

    close(conn);
end
