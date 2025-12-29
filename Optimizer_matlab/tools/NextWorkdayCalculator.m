function next_workday = NextWorkdayCalculator(available_date)
    % 计算下一个工作日

    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);
    

    host = db_config.host;
    dbname = db_config.dbname;
    username = db_config.username;
    password = db_config.password;
    conn = database(dbname, username, password, ...
                    'com.mysql.cj.jdbc.Driver', ...
                    ['jdbc:mysql://' host '/' dbname]);

    if isdatetime(available_date)
        available_date_str = string(available_date, 'yyyy-MM-dd');
    elseif ischar(available_date) || isstring(available_date)
        available_date_str = char(available_date);
    end

    % query = ['SELECT valuation_date FROM data_prepared_new.chinesevaluationdate ' ...
    %          'WHERE valuation_date > ''', available_date_str, ''' ' ...
    %          'ORDER BY valuation_date ASC LIMIT 1'];
    
    query = "SELECT valuation_date " + ...
        "FROM data_prepared_new.chinesevaluationdate " + ...
        "WHERE valuation_date > '" + available_date_str + "' " + ...
        "ORDER BY valuation_date ASC LIMIT 1";


    result = fetch(conn, query);
    close(conn);
    
   next_workday = string(result.valuation_date(1));
   
end

