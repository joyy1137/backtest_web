function [is_trading_day, actual_date] = ValidateWorkingDay(date)
    %验证是否为交易日，如果不是则返回最近的前一个交易日
    %   输入: date - 要验证的日期 (datetime, char, string)
    %   输出: is_trading_day - 是否为交易日 (logical)
    %         actual_date - 实际使用的交易日 (string, 格式: yyyy-MM-dd)
    
    % 从 config_db.m 文件读取数据库配置
    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);
    
    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);
    
    % 标准化日期格式为 yyyy-MM-dd
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
        % 首先检查是否为交易日
        check_query = ['SELECT valuation_date FROM data_prepared_new.chinesevaluationdate ', ...
                      'WHERE valuation_date = ''', date_str, ''''];
        result = fetch(conn, check_query);
        
        if ~isempty(result)
            % 是交易日
            is_trading_day = true;
            actual_date = date_str;
        else
            % 不是交易日，查找最近的前一个交易日
            is_trading_day = false;
            fallback_query = ['SELECT valuation_date FROM data_prepared_new.chinesevaluationdate ', ...
                             'WHERE valuation_date < ''', date_str, ''' ', ...
                             'ORDER BY valuation_date DESC LIMIT 1'];
            fallback_result = fetch(conn, fallback_query);
            
            if ~isempty(fallback_result)
                actual_date = char(fallback_result.valuation_date(1));
                
            else
                actual_date = date_str;
                fprintf('WARNING: 未找到 %s 之前的交易日，使用原始日期\n', date_str);
            end
        end
        
    catch ME
        close(conn);
        rethrow(ME);
    end
    close(conn);
end
