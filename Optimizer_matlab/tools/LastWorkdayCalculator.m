function last_workday = LastWorkdayCalculator(target_date)
    % 计算上一个工作日

    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);

    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);

    
    if isdatetime(target_date)
        target_date_str = datestr(target_date, 'yyyy-mm-dd');
    elseif ischar(target_date) || isstring(target_date)
        target_date_str = char(target_date);
        % 验证日期格式
        if ~isempty(regexp(target_date_str, '^\d{4}-\d{2}-\d{2}$', 'once'))
            % 格式正确
        else
            error('日期格式不正确，应为 yyyy-MM-dd');
        end
    else
        error('不支持的日期格式');
    end
    
    % 检查 target_date_str 是否为空
    if isempty(target_date_str)
        error('目标日期字符串为空');
    end
    
    query = ['SELECT valuation_date FROM data_prepared_new.chinesevaluationdate ' ...
             'WHERE valuation_date < ''', target_date_str, ''' ' ...
             'ORDER BY valuation_date DESC LIMIT 1'];

    result = fetch(conn, query);
    close(conn);

    last_workday = string(result.valuation_date(1));
    
end
