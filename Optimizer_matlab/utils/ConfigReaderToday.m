function [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path, useToday, specifiedDate)
% ConfigReaderToday -
%
% 用法:
%   [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path)
%   [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path, true)
%   [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReaderToday(config_path, false)
%
% 说明:
%   - 本函数会调用现有的 ConfigReader 函数读取配置，然后根据第二个参数
%     决定是否将 portfolio_info 中的 start_date / end_date 覆盖为今天。
%
% 参数:
%   config_path   - 配置文件路径（通常为 fullfile(script_dir, 'config', 'opt_project_config.xlsx')）
%   useToday      - 可选，true/false（默认为 true）
%   specifiedDate - 可选，指定用于覆盖的日期（datetime 或 yyyy-MM-dd 字符串）。
%                   如果提供了本参数，则优先使用该日期覆盖 start_date/end_date，
%                   无论 useToday 为何值。

    if nargin < 2 || isempty(useToday)
        useToday = true;
    end
    if nargin < 3
        specifiedDate = [];
    end


    [portfolio_info, portfolio_constraint, factor_constraint] = ConfigReader(config_path);

   
    if isempty(specifiedDate) && ~useToday
        return;
    end

    % 解析指定日期或使用今天
    if ~isempty(specifiedDate)
        if isdatetime(specifiedDate)
            override_dt = specifiedDate;
        else
            try
                % 尝试按 yyyy-MM-dd 解析字符串
                override_dt = datetime(char(specifiedDate), 'InputFormat', 'yyyy-MM-dd');
            catch
                % 回退到通用解析
                override_dt = datetime(char(specifiedDate));
            end
        end
    else

        today_dt = datetime('today');

       
        [is_trading, actual_date] = ValidateWorkingDay(today_dt);
      
        now_dt = datetime('now');
        
        if is_trading && hour(now_dt) >= 23
            override_dt = NextWorkdayCalculator(today_dt);
        elseif ~is_trading
            last = datetime(actual_date, 'InputFormat', 'yyyy-MM-dd');
            override_dt = NextWorkdayCalculator(last);
        else
            override_dt = today_dt;
            
        end
        
    end

    if ismember('start_date', portfolio_info.Properties.VariableNames)
        try
            portfolio_info.start_date(:) = repmat(override_dt, height(portfolio_info), 1);
        catch
            portfolio_info.start_date = repmat(override_dt, height(portfolio_info), 1);
        end
    else
        portfolio_info.start_date = repmat(override_dt, height(portfolio_info), 1);
    end

    if ismember('end_date', portfolio_info.Properties.VariableNames)
        try
            portfolio_info.end_date(:) = repmat(override_dt, height(portfolio_info), 1);
        catch
            portfolio_info.end_date = repmat(override_dt, height(portfolio_info), 1);
        end
    else
        portfolio_info.end_date = repmat(override_dt, height(portfolio_info), 1);
    end

    % 在添加日期后，再次尝试写入数据库
    
    try
        write_portfolio_info_to_db(portfolio_info);
    catch ME
        warning('写入数据库失败: %s', ME.message);
    end

    config_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'config', 'config_db.m');
    run(config_path);
    
end