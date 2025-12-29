function [df_stock_return] = stock_return_withdraw(start_date, end_date)
    %STOCK_RETURN_WITHDRAW 提取股票收益率数据
    %   从数据库中提取指定日期范围内的股票收益率数据
    
    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);

    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);

    % 简化日期处理逻辑
    if isdatetime(start_date)
        start_date_str = datestr(start_date, 'yyyy-mm-dd');
    else
        start_date_str = char(start_date);
    end
    
    if isdatetime(end_date)
        end_date_str = datestr(end_date, 'yyyy-mm-dd');
    else
        end_date_str = char(end_date);
    end

    

    
    
    % 查询指定日期范围的数据
    query = sprintf('SELECT valuation_date, code, pct_chg FROM data_prepared_new.data_stock WHERE valuation_date BETWEEN ''%s'' AND ''%s''', start_date_str, end_date_str);
    df_stock_return = fetch(conn, query);
    
    % 确保数据类型正确
    if height(df_stock_return) > 0
        
        % 确保valuation_date是字符串类型
        if iscell(df_stock_return.valuation_date)
            % 将cell数组转换为字符串数组
            df_stock_return.valuation_date = string(df_stock_return.valuation_date);
        end
        
        % 确保code是字符串类型
        if iscell(df_stock_return.code)
            % 将cell数组转换为字符串数组
            df_stock_return.code = string(df_stock_return.code);
        end
    end
    if height(df_stock_return) > 0
        unique_dates = unique(df_stock_return.valuation_date);
    else
        % 如果没找到数据，尝试查询最近的数据
        recent_query = 'SELECT valuation_date, code, pct_chg FROM data_prepared_new.data_stock ORDER BY valuation_date DESC LIMIT 10';
        recent_result = fetch(conn, recent_query);
        if height(recent_result) > 0
            unique_dates = unique(recent_result.valuation_date);
        end
    end
    close(conn);
end
