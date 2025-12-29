classdef BacktestToolbox < handle
    %BACKTESTTOOLBOX 投资组合回测工具箱
    %   这是一个完整的投资组合回测分析工具箱，包含基准净值计算、
    %   投资组合净值计算、业绩指标分析、成分股贡献分析等功能。
    %
    %   主要功能：
    %   - 基准净值计算
    %   - 投资组合净值计算（含换手率成本）
    %   - 业绩指标计算（年化收益率、夏普比率、信息比率、最大回撤等）
    %   - 成分股贡献分析
    %   - 图表生成和PDF报告
    %
   
    
    properties (Access = private)
        input_path = '';   
        output_path = ''; 
        db_connector = [];
        portfolio_info = [];
        portfolio_constraint = [];
        factor_constraint = [];
        user_name = '';
        portfolio_name = '';
        start_date = '';
        end_date = '';
        
    end
    
    methods
        function obj = BacktestToolbox()
            %BACKTESTTOOLBOX 构造函数
            
        end
        
        function date_str = formatDate(obj, date_input, format)
            %FORMATDATE 兼容不同MATLAB版本的日期格式化
            %   输入: date_input - 日期输入
            %        format - 格式字符串
            %   输出: date_str - 格式化后的日期字符串
            if nargin < 3
                format = 'yyyy-mm-dd';
            end
            
            if isdatetime(date_input)
                % 使用 datestr 函数确保正确的日期格式化
                if strcmp(format, 'yyyy-mm-dd')
                    date_str = datestr(date_input, 'yyyy-mm-dd');
                elseif strcmp(format, 'yyyymmdd')
                    date_str = datestr(date_input, 'yyyymmdd');
                else
                    date_str = datestr(date_input, format);
                end
            else
                date_str = char(date_input);
                if strcmp(format, 'yyyymmdd')
                    date_str = strrep(strrep(strrep(date_str, '-', ''), '/', ''), ' ', '');
                end
            end
        end
        
    
        
        function setInputPath(obj, input_path)
            %SETINPUTPATH 设置输入路径
            %   输入: input_path - 输入目录路径（用于读取投资组合数据）
            obj.input_path = input_path;
            fprintf('输入路径已设置: %s\n', input_path);
        end
        
        function setOutputPath(obj, output_path)
            %SETOUTPUTPATH 设置输出路径
           
            obj.output_path = output_path;
            fprintf('输出路径已设置: %s\n', output_path);
        end
        
        function setCurrentPortfolio(obj, portfolio_name, user_name, start_date, end_date)
            %SETCURRENTPORTFOLIO 设置当前投资组合信息
            %   输入: portfolio_name - 投资组合名称
            %        user_name - 用户名称
            %        start_date - 开始日期
            %        end_date - 结束日期
            obj.portfolio_name = portfolio_name;
            obj.user_name = user_name;
            obj.start_date = start_date;
            obj.end_date = end_date;
            fprintf('当前投资组合已设置: %s (%s, %s 到 %s)\n', ...
                portfolio_name, user_name, start_date, end_date);
        end
        
        function runBacktest(obj)
            %RUNBACKTEST 运行回测分析
            %   执行完整的回测分析流程
            
        
            
            fprintf('\n=== 开始投资组合回测分析 ===\n');
            
            try
                % 1. 加载配置
                obj.loadConfiguration();
                
                % 2. 初始化数据库连接
                obj.initializeDatabase();
                
                % 3. 运行回测
                obj.executeBacktest();
                
                fprintf('\n=== 回测分析完成 ===\n');
                
            catch ME
                fprintf('回测过程中出现错误: %s\n', ME.message);
                if ~isempty(ME.stack)
                    fprintf('错误位置: %s (第%d行)\n', ME.stack(1).name, ME.stack(1).line);
                end
                rethrow(ME);
            end
        end
        
        function loadConfiguration(obj)
            %LOADCONFIGURATION 加载配置文件
            % 添加utils路径
            utils_path = fullfile(fileparts(fileparts(mfilename('fullpath'))), 'utils');
            addpath(utils_path);
            
            [obj.portfolio_info, obj.portfolio_constraint, obj.factor_constraint] = ...
                ConfigReader_sql();
            
            % 转换日期格式
            obj.portfolio_info.start_date = datetime(obj.portfolio_info.start_date, 'InputFormat', 'yyyy-MM-dd');
            obj.portfolio_info.end_date = datetime(obj.portfolio_info.end_date, 'InputFormat', 'yyyy-MM-dd');
            obj.user_name = obj.portfolio_info.user_name{1};
            fprintf('配置文件加载完成\n');
        end
        
        function initializeDatabase(obj)
            %INITIALIZEDATABASE 初始化数据库连接
            obj.db_connector = DatabaseConnector();
            fprintf('数据库连接初始化完成\n');
        end
        
        function executeBacktest(obj)
            %EXECUTEBACKTEST 执行回测分析
            % 如果已经设置了特定的投资组合信息，使用它们；否则从配置文件读取
            if ~isempty(obj.portfolio_name) && ~isempty(obj.user_name) && ~isempty(obj.start_date) && ~isempty(obj.end_date)
                % 使用已设置的投资组合信息
                start_date = obj.start_date;
                if isdatetime(start_date)
                    dt = start_date;   % 已经是 datetime，不用转换
                elseif ischar(start_date) || isstring(start_date)
                    if ~isempty(regexp(start_date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                        % yyyy-MM-dd 格式，例如 2025-07-01
                        dt = datetime(start_date, 'InputFormat', 'yyyy-MM-dd');
                    elseif ~isempty(regexp(start_date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                        % dd-MMM-yyyy 格式，例如 01-Jul-2025
                        dt = datetime(start_date, 'InputFormat', 'dd-MMM-yyyy', 'Locale', 'en_US');
                    else
                        error('Unrecognized date format: %s', start_date);
                    end
                else
                    error('Unsupported type for start_date');
                end

                start_date = string(dt, 'yyyy-MM-dd');
                        

                end_date = obj.end_date;
                if isdatetime(end_date)
                    dt = end_date;
                elseif ischar(end_date) || isstring(end_date)
                    if ~isempty(regexp(end_date, '^\d{4}-\d{2}-\d{2}$', 'once'))
                        dt = datetime(end_date, 'InputFormat', 'yyyy-MM-dd');
                    elseif ~isempty(regexp(end_date, '^\d{2}-[A-Za-z]{3}-\d{4}$', 'once'))
                        dt = datetime(end_date, 'InputFormat', 'dd-MMM-yyyy', 'Locale', 'en_US');
                    else
                        error('Unrecognized date format: %s', end_date);
                    end
                else
                    error('Unsupported type for end_date');
                end
                end_date = string(dt, 'yyyy-MM-dd'); 

                portfolio_name = obj.portfolio_name;

                % 根据 portfolio_name 查找对应的 index_type
                row_idx = find(strcmp(obj.portfolio_info.portfolio_name, portfolio_name), 1);
                if isempty(row_idx)
                    error('未找到投资组合 %s 的配置信息', portfolio_name);
                end
                index_type = obj.portfolio_info.index_type{row_idx};
            else
                % 从配置文件读取（原有逻辑）
                start_date = min(obj.portfolio_info.start_date);
                dt = datetime(start_date, 'InputFormat', 'dd-MMM-yyyy');  
                start_date = string(dt, 'yyyy-MM-dd'); 

                end_date = max(obj.portfolio_info.end_date);
                dt = datetime(end_date, 'InputFormat', 'dd-MMM-yyyy');  
                end_date = string(dt, 'yyyy-MM-dd'); 


                index_type = obj.portfolio_info.index_type{1};
                portfolio_name = obj.portfolio_info.portfolio_name{1};
            end
            
            fprintf('回测参数:\n');
            fprintf('  投资组合: %s\n', portfolio_name);
            fprintf('  指数类型: %s\n', index_type);
            
    
            fprintf('  开始日期: %s\n', obj.formatDate(start_date));
            fprintf('  结束日期: %s\n', obj.formatDate(end_date));
            
       
           
            % 提取指数成分股信息
            df_components = obj.db_connector.index_component_withdraw(start_date, index_type);
            fprintf('指数成分股数量: %d\n', height(df_components));
            
            % 计算基准净值
            [benchmark_net_value, benchmark_dates] = obj.calculateBenchmarkNetValue(...
                index_type, start_date, end_date, df_components);
            
            % 计算投资组合净值
            cost_rate = 0.00085; 
            inputpath_backtesting = fullfile(obj.input_path, obj.user_name, portfolio_name);
            
           
            
            if exist(inputpath_backtesting, 'dir')
                fprintf('找到投资组合数据\n');
                [portfolio_net_value, portfolio_dates, turnover_data, portfolio_returns] = ...
                    BacktestToolbox.calculate_portfolio_net_value_unified(obj.db_connector, index_type, start_date, end_date, ...
                    inputpath_backtesting, cost_rate);
            else
                fprintf('⚠ 投资组合数据路径不存在: %s\n', inputpath_backtesting);
                % 初始化空变量，避免后续错误
                portfolio_net_value = [];
                portfolio_dates = [];
                turnover_data = [];
                return; % 直接返回，不继续执行后续分析
            end
            
            % 生成结果表格
            result_table = obj.createResultTable(benchmark_dates, benchmark_net_value, ...
                portfolio_dates, portfolio_net_value);
            
            % 计算业绩指标
            performance_metrics = obj.calculatePerformanceMetrics(result_table, portfolio_returns, portfolio_dates, index_type, start_date, end_date);
            
            % 导出结果
            obj.exportResults(result_table, performance_metrics, portfolio_name, ...
                start_date, end_date, inputpath_backtesting);
            
            
            % 找到与当前投资组合名称匹配的列
            portfolio_cols = obj.portfolio_constraint.Properties.VariableNames;
            matching_col = '';
            for i = 1:length(portfolio_cols)
                if strcmp(portfolio_cols{i}, portfolio_name)
                    matching_col = portfolio_cols{i};
                    break;
                end
            end
            
            if ~isempty(matching_col)
                fprintf('  找到匹配的列: %s\n', matching_col);
                top_number_cell = obj.portfolio_constraint.(matching_col)(strcmp(obj.portfolio_constraint.constraint_name, 'top_number'));
                
                if ~isempty(top_number_cell) && ~isempty(top_number_cell{1})
                    raw_value = top_number_cell{1};
                    if ischar(raw_value) || isobj.formatDate(raw_value)
                        top_number = str2double(raw_value);
                    else
                        top_number = raw_value;
                    end
                    fprintf('  获取到的top_number值: %d\n', top_number);
                else
                    top_number = [];
                    fprintf('  警告: 未找到top_number值\n');
                end
            else
                fprintf('  警告: 未找到与投资组合名称 %s 匹配的列\n', portfolio_name);
                top_number = [];
            end
            
            if isempty(top_number)
                fprintf('  警告: 未找到有效的top_number值，无法进行成分股贡献分析\n');
                fprintf('  调试信息:\n');
                fprintf('    - 投资组合名称: %s\n', portfolio_name);
              
                fprintf('    - 可用的列名: %s\n', strjoin(portfolio_cols, ', '));
                if ~isempty(obj.portfolio_constraint)
                    fprintf('    - 约束表行数: %d\n', height(obj.portfolio_constraint));
                    if ismember('constraint_name', obj.portfolio_constraint.Properties.VariableNames)
                        fprintf('    - 约束名称: %s\n', strjoin(unique(obj.portfolio_constraint.constraint_name), ', '));
                    end
                else
                    fprintf('    - 约束表为空\n');
                end
                return;
            end
            
            obj.calculateContributionAnalysis(portfolio_dates, inputpath_backtesting, ...
                df_components, index_type, portfolio_name, start_date, end_date, top_number);
            
            % 生成PDF报告
            obj.generatePDFReport(result_table, performance_metrics, portfolio_name, ...
                index_type, start_date, end_date);
        end
        
        function [benchmark_net_value, benchmark_dates] = calculateBenchmarkNetValue(obj, index_type, start_date, end_date, df_components)
            %CALCULATEBENCHMARKNETVALUE 计算基准净值
            [benchmark_net_value, benchmark_dates] = BacktestToolbox.calculate_benchmark_net_value(...
                obj.db_connector, index_type, start_date, end_date, df_components);
        end
        
        function [portfolio_net_value, portfolio_dates, turnover_data, portfolio_returns] = calculatePortfolioNetValue(obj, index_type, start_date, end_date, df_components, cost_rate)
            %CALCULATEPORTFOLIONETVALUE 计算投资组合净值
            data_source = struct('df_components', df_components);
            [portfolio_net_value, portfolio_dates, turnover_data, portfolio_returns] = BacktestToolbox.calculate_portfolio_net_value_unified(...
                obj.db_connector, index_type, start_date, end_date, data_source, cost_rate);
        end
        
        function [portfolio_net_value, portfolio_dates, turnover_data, portfolio_returns] = calculatePortfolioNetValueWithFiles(obj, index_type, start_date, end_date, inputpath_backtesting, cost_rate)
            %CALCULATEPORTFOLIONETVALUEWITHFILES 
            [portfolio_net_value, portfolio_dates, turnover_data, portfolio_returns] = BacktestToolbox.calculate_portfolio_net_value_unified(...
                obj.db_connector, index_type, start_date, end_date, inputpath_backtesting, cost_rate);
        end
        
        function result_table = createResultTable(obj, benchmark_dates, benchmark_net_value, portfolio_dates, portfolio_net_value)
            %CREATERESULTTABLE 创建结果表格
            
            
            result_table = table();
            result_table.valuation_date = benchmark_dates;
            result_table.benchmark_net_value = benchmark_net_value;
            
            % 对齐投资组合净值
            if length(portfolio_net_value) == length(benchmark_net_value)
                result_table.portfolio_net_value = portfolio_net_value;
            else
                portfolio_aligned = NaN(length(benchmark_net_value), 1);
                min_length = min(length(portfolio_net_value), length(benchmark_net_value));
                portfolio_aligned(1:min_length) = portfolio_net_value(1:min_length);
                result_table.portfolio_net_value = portfolio_aligned;
            end
            
            % 计算超额净值（组合净值减去基准净值）
            % 找到投资组合净值和基准净值都有效的数据点
            valid_portfolio = ~isnan(result_table.portfolio_net_value);
            valid_benchmark = ~isnan(result_table.benchmark_net_value);
            valid_data = valid_portfolio & valid_benchmark;
            
            % 初始化超额净值数组 - 使用基准净值的长度
            excess_net_value = NaN(length(result_table.benchmark_net_value), 1);
            
            % 计算超额收益率并累积乘积得到超额净值
            if sum(valid_data) > 0
                % 获取有效数据
                portfolio_valid = result_table.portfolio_net_value(valid_data);
                benchmark_valid = result_table.benchmark_net_value(valid_data);
                
                % 计算超额收益率 - 与Python版本一致
                % 从净值计算每日收益率
                portfolio_returns_daily = [portfolio_valid(1) - 1; diff(portfolio_valid) ./ portfolio_valid(1:end-1)];
                benchmark_returns_daily = [benchmark_valid(1) - 1; diff(benchmark_valid) ./ benchmark_valid(1:end-1)];
                
                % 第1天超额收益率 = 组合收益率₁ - 基准收益率₁
                excess_returns_daily = portfolio_returns_daily - benchmark_returns_daily;
                
                % 计算超额净值：(1 + 超额收益率).cumprod() - 与Python版本一致
                % 第1天: 1 + 超额收益率₁, 第2天: (1 + 超额收益率₁) × (1 + 超额收益率₂), etc.
                excess_cumprod = cumprod(1 + excess_returns_daily);
                
                % 将结果放回原数组
                valid_indices = find(valid_data);
                excess_net_value(valid_indices) = excess_cumprod;
            end
            
            result_table.excess_net_value = excess_net_value;
        end
        
        function performance_metrics = calculatePerformanceMetrics(obj, result_table, portfolio_returns, portfolio_dates, index_type, start_date, end_date)
            %CALCULATEPERFORMANCEMETRICS 计算业绩指标
            
            % 找到有效数据点
            valid_portfolio = ~isnan(result_table.portfolio_net_value);
            valid_benchmark = ~isnan(result_table.benchmark_net_value);
            valid_data = valid_portfolio & valid_benchmark;
            
            
            if sum(valid_data) < 2
                % 如果有效数据点太少，返回默认值
                performance_metrics = table();
                metric_names = {'Annual_Return_Pct', 'Sharpe_Ratio', 'Info_Ratio', 'Max_Drawdown_Pct', 'Annual_Vol_Pct'};
                metric_values = [0; 0; 0; 0; 0];
                performance_metrics.Metric = metric_names';
                performance_metrics.Value = metric_values;
                fprintf('警告: 有效数据点不足，返回默认业绩指标\n');
                return;
            end
            
            % 使用有效数据计算收益率
            portfolio_valid = result_table.portfolio_net_value(valid_data);
            benchmark_valid = result_table.benchmark_net_value(valid_data);
            
            % 优先使用result_table中已经计算好的超额净值来反推超额收益率
            % 这样可以确保和超额净值计算完全一致
            excess_net_value_valid = result_table.excess_net_value(valid_data);
            use_excess_nav = false;
            if ~isempty(excess_net_value_valid) && sum(~isnan(excess_net_value_valid)) > 1
                % 从超额净值反推超额收益率（与createResultTable中的计算一致）
                excess_returns_daily = [];
                % 初始化为datetime类型的空数组
                valid_dates_for_excess = portfolio_dates(valid_data);
                if isdatetime(valid_dates_for_excess)
                    excess_dates = datetime.empty(0,1);
                elseif iscell(valid_dates_for_excess)
                    excess_dates = {};
                else
                    excess_dates = [];
                end
                
                for i = 2:length(excess_net_value_valid)
                    if ~isnan(excess_net_value_valid(i)) && ~isnan(excess_net_value_valid(i-1)) && excess_net_value_valid(i-1) ~= 0
                        % 从超额净值计算超额收益率
                        excess_return = (excess_net_value_valid(i) - excess_net_value_valid(i-1)) / excess_net_value_valid(i-1);
                        excess_returns_daily(end+1) = excess_return;
                        % 记录对应日期，确保类型匹配
                        date_to_add = valid_dates_for_excess(i);
                        if isdatetime(excess_dates)
                            if isdatetime(date_to_add)
                                excess_dates(end+1) = date_to_add;
                            else
                                try
                                    excess_dates(end+1) = datetime(date_to_add);
                                catch
                                    excess_dates(end+1) = datetime(string(date_to_add), 'InputFormat', 'yyyy-MM-dd');
                                end
                            end
                        elseif iscell(excess_dates)
                            excess_dates{end+1} = date_to_add;
                        else
                            excess_dates(end+1) = date_to_add;
                        end
                    end
                end
                if length(excess_returns_daily) > 0
                    use_excess_nav = true;
                    fprintf('✓ 使用result_table中的超额净值反推超额收益率，共 %d 个数据点\n', length(excess_returns_daily));
                end
            end
            
            if ~use_excess_nav
                % 如果无法从超额净值反推，使用原来的方法计算
                fprintf('使用原始方法计算超额收益率\n');
            
            % 计算超额收益率：直接使用组合收益率和基准收益率
            df_index_return = BacktestToolbox.index_return_withdraw(start_date, end_date);
            
            if isempty(df_index_return)
                % 使用净值数据计算超额收益率
                excess_returns_daily = [];
                % 初始化为datetime类型的空数组
                if isdatetime(portfolio_dates)
                    excess_dates = datetime.empty(0,1);
                else
                    excess_dates = [];  % 如果不是datetime，使用普通数组
                end
                valid_dates_for_excess = portfolio_dates(valid_data);
                for i = 2:length(portfolio_valid)
                    portfolio_return = (portfolio_valid(i) - portfolio_valid(i-1)) / portfolio_valid(i-1);
                    benchmark_return = (benchmark_valid(i) - benchmark_valid(i-1)) / benchmark_valid(i-1);
                    excess_return = portfolio_return - benchmark_return;
                    excess_returns_daily(end+1) = excess_return;
                    % 记录对应日期，确保类型匹配
                    date_to_add = valid_dates_for_excess(i);
                    if isdatetime(excess_dates)
                        if isdatetime(date_to_add)
                            excess_dates(end+1) = date_to_add;
                        else
                            % 如果不是datetime，尝试转换
                            try
                                excess_dates(end+1) = datetime(date_to_add);
                            catch
                                excess_dates(end+1) = datetime(string(date_to_add), 'InputFormat', 'yyyy-MM-dd');
                            end
                        end
                    else
                        excess_dates(end+1) = date_to_add;
                    end
                end
            else
                
                % 转换指数类型为代码
                switch index_type
                    case {'沪深300', 'hs300'}
                        index_code = '000300.SH';
                    case {'中证500', 'zz500'}
                        index_code = '000905.SH';
                    case {'中证1000', 'zz1000'}
                        index_code = '000852.SH';
                    case {'中证2000', 'zz2000'}
                        index_code = '932000.CSI';
                    case {'中证A500', 'zzA500'}
                        index_code = '000510.CSI';
                    otherwise
                        error('不支持的指数类型: %s', index_type);
                end
                
                % 筛选对应指数的收益率数据
                % 确保数据类型匹配
                if iscell(df_index_return.code)
                    index_data = df_index_return(strcmp(df_index_return.code, index_code), :);
                else
                    index_data = df_index_return(df_index_return.code == index_code, :);
                end
                index_data = sortrows(index_data, 'valuation_date');
            
                % 对齐日期并计算超额收益率
                excess_returns_daily = [];
                % 初始化为datetime类型的空数组
                if isdatetime(portfolio_dates)
                    excess_dates = datetime.empty(0,1);
                else
                    excess_dates = [];  % 如果不是datetime，使用普通数组
                end
                
                % 确保基准数据日期格式正确
                if ~isempty(index_data)
                    if isdatetime(index_data.valuation_date)
                        for j = 1:length(index_data.valuation_date)
                            index_data.valuation_date{j} = string(index_data.valuation_date(j));
                        end
                    end
                end
                
                for i = 1:length(portfolio_dates)
                    current_date = portfolio_dates(i);
                    current_date_str = string(current_date);
                    
                    % 找到对应的基准收益率
                    benchmark_idx = false(size(index_data.valuation_date));
                    if ~isempty(index_data)
                        % 确保数据类型匹配
                        if iscell(index_data.valuation_date)
                            benchmark_idx = strcmp(index_data.valuation_date, current_date_str);
                        else
                            benchmark_idx = index_data.valuation_date == current_date_str;
                        end
                        
                        if ~any(benchmark_idx)
                            alt_date_str = string(current_date);
                            % 确保数据类型匹配
                            if iscell(index_data.valuation_date)
                                benchmark_idx = strcmp(index_data.valuation_date, alt_date_str);
                            else
                                benchmark_idx = index_data.valuation_date == alt_date_str;
                            end
                        end
                        
                        if ~any(benchmark_idx) && isdatetime(index_data.valuation_date)
                            benchmark_idx = (index_data.valuation_date == current_date);
                        end
                    end
                    
                    if any(benchmark_idx)
                        benchmark_return = index_data.pct_chg(benchmark_idx);
                        portfolio_return = portfolio_returns(i);
                        excess_return = portfolio_return - benchmark_return;
                        excess_returns_daily(end+1) = excess_return;
                        % 记录对应日期，确保类型匹配
                        if isdatetime(excess_dates)
                            if isdatetime(current_date)
                                excess_dates(end+1) = current_date;
                            else
                                % 如果不是datetime，尝试转换
                                try
                                    excess_dates(end+1) = datetime(current_date);
                                catch
                                    excess_dates(end+1) = datetime(string(current_date), 'InputFormat', 'yyyy-MM-dd');
                                end
                            end
                        else
                            excess_dates(end+1) = current_date;
                        end
                    end
                end
            end
            end  % 结束 if ~use_excess_nav 块
            
            
            % 过滤掉无效值，同时过滤对应的日期
            valid_excess_mask = isfinite(excess_returns_daily);
            excess_returns_daily = excess_returns_daily(valid_excess_mask);
            if exist('excess_dates', 'var') && ~isempty(excess_dates)
                excess_dates = excess_dates(valid_excess_mask);
            end
            
            total_days = sum(valid_data);
            trading_days = length(excess_returns_daily);  % 实际交易日数
            
           
            % 如果超额收益率数据不足，使用净值数据计算
            if isempty(excess_returns_daily) || trading_days < 2
                fprintf('警告: 超额收益率数据不足，使用净值数据计算业绩指标\n');
                
                % 使用净值数据计算收益率
                portfolio_returns_from_nav = [];
                benchmark_returns_from_nav = [];
                valid_dates_for_nav = portfolio_dates(valid_data);
                
                for i = 2:length(portfolio_valid)
                    portfolio_return = (portfolio_valid(i) - portfolio_valid(i-1)) / portfolio_valid(i-1);
                    benchmark_return = (benchmark_valid(i) - benchmark_valid(i-1)) / benchmark_valid(i-1);
                    portfolio_returns_from_nav(end+1) = portfolio_return;
                    benchmark_returns_from_nav(end+1) = benchmark_return;
                end
                
                excess_returns_daily = portfolio_returns_from_nav - benchmark_returns_from_nav;
                % 记录对应的日期（从第二天开始）
                if length(valid_dates_for_nav) > 1
                    excess_dates = valid_dates_for_nav(2:end);
                else
                    % 如果日期不足，初始化为datetime类型的空数组
                    if isdatetime(valid_dates_for_nav)
                        excess_dates = datetime.empty(0,1);
                    else
                        excess_dates = [];
                    end
                end
                trading_days = length(excess_returns_daily);
            end
            
            % 年化收益率：按年份分别计算
            if ~isempty(excess_returns_daily) && trading_days > 0
                % 获取对应的日期信息（用于按年份分组）
                % 优先使用记录的excess_dates，如果没有则从valid_data获取
                if exist('excess_dates', 'var') && ~isempty(excess_dates) && length(excess_dates) == length(excess_returns_daily)
                    valid_dates = excess_dates;
                else
                    % 回退到从valid_data获取日期
                    valid_dates = portfolio_dates(valid_data);
                    % 根据长度差异判断日期对应关系
                    if length(valid_dates) == length(excess_returns_daily) + 1
                        % 从净值计算的情况：收益率从第二天开始，日期也要从第二天开始
                        valid_dates = valid_dates(2:end);
                    elseif length(valid_dates) > length(excess_returns_daily)
                        % 如果日期更多，取后面的部分
                        valid_dates = valid_dates(end-length(excess_returns_daily)+1:end);
                    elseif length(valid_dates) < length(excess_returns_daily)
                        % 如果日期更少，取收益率的前面部分
                        excess_returns_daily = excess_returns_daily(end-length(valid_dates)+1:end);
                    end
                end
                
                % 确保日期和收益率数量一致
                min_len = min(length(excess_returns_daily), length(valid_dates));
                excess_returns_daily = excess_returns_daily(1:min_len);
                valid_dates = valid_dates(1:min_len);
                
                % 按年份分组计算年化收益率
                if isdatetime(valid_dates)
                    years = year(valid_dates);
                elseif iscell(valid_dates)
                    years = cellfun(@(x) year(datetime(x, 'InputFormat', 'yyyy-MM-dd')), valid_dates);
                else
                    years = year(datetime(valid_dates, 'InputFormat', 'yyyy-MM-dd'));
                end
                
                unique_years = unique(years);
                annual_returns_by_year = [];
                sharpe_by_year = [];
                info_ratio_by_year = [];
                max_dd_by_year = [];
                vol_by_year = [];
                trading_days_by_year = [];
                
                % 预先计算组合和基准收益率（用于信息比率计算）
                portfolio_returns_daily = [];
                benchmark_returns_daily = [];
                if ~isempty(portfolio_returns) && ~isempty(benchmark_valid)
                    for i = 2:length(portfolio_valid)
                        portfolio_return = (portfolio_valid(i) - portfolio_valid(i-1)) / portfolio_valid(i-1);
                        benchmark_return = (benchmark_valid(i) - benchmark_valid(i-1)) / benchmark_valid(i-1);
                        portfolio_returns_daily(end+1) = portfolio_return;
                        benchmark_returns_daily(end+1) = benchmark_return;
                    end
                end
                
                fprintf('\n=== 按年份计算业绩指标 ===\n');
                for y = 1:length(unique_years)
                    year_mask = (years == unique_years(y));
                    year_returns = excess_returns_daily(year_mask);
                    year_trading_days = sum(year_mask);
                    
                    if ~isempty(year_returns) && year_trading_days > 0
                        % 计算该年的年化收益率
                        year_cumprod = cumprod(1 + year_returns);
                        if length(year_cumprod) > 0
                            year_total_return = year_cumprod(end) - 1;
                            % 年化收益率 = 总收益率 * (252 / 实际交易日数)
                            year_annual_return = year_total_return * (252 / year_trading_days);
                            annual_returns_by_year(end+1) = year_annual_return;
                            
                            % 计算该年的年化标准差
                            year_vol = std(year_returns) * sqrt(252);
                            vol_by_year(end+1) = year_vol;
                            
                            % 计算该年的夏普比率
                            if year_vol > 0
                                year_sharpe = round(year_annual_return / year_vol, 2);
                            else
                                year_sharpe = 0;
                            end
                            sharpe_by_year(end+1) = year_sharpe;
                            
                            % 计算该年的最大回撤
                            year_excess_nav = cumprod(1 + year_returns);
                            year_nav_max = cummax(year_excess_nav);
                            year_drawdowns = (year_nav_max - year_excess_nav) ./ year_nav_max;
                            year_max_dd = max(year_drawdowns);
                            max_dd_by_year(end+1) = year_max_dd;
                            
                            % 计算该年的信息比率（需要组合和基准收益率）
                            % 获取该年的组合和基准收益率
                            year_portfolio_returns = [];
                            year_benchmark_returns = [];
                            if ~isempty(portfolio_returns_daily) && ~isempty(benchmark_returns_daily)
                                % 找到对应的日期索引（excess_returns_daily的索引）
                                year_date_indices = find(year_mask);
                                % 确保索引在有效范围内
                                valid_indices = year_date_indices(year_date_indices <= length(portfolio_returns_daily) & ...
                                    year_date_indices <= length(benchmark_returns_daily));
                                if ~isempty(valid_indices)
                                    year_portfolio_returns = portfolio_returns_daily(valid_indices);
                                    year_benchmark_returns = benchmark_returns_daily(valid_indices);
                                end
                            end
                            
                            % 筛选出基金和基准收益率都为正的数据点
                            if ~isempty(year_portfolio_returns) && ~isempty(year_benchmark_returns)
                                year_positive_mask = (year_portfolio_returns > 0) & (year_benchmark_returns > 0);
                                if sum(year_positive_mask) > 0
                                    year_positive_excess = year_returns(year_positive_mask);
                                    if ~isempty(year_positive_excess) && year_vol > 0
                                        year_positive_cumprod = cumprod(1 + year_positive_excess);
                                        if length(year_positive_cumprod) > 0
                                            year_positive_total_return = year_positive_cumprod(end) - 1;
                                            % 年化收益率 = 总收益率 * (252 / 实际交易日数)
                                            year_positive_annual_return = year_positive_total_return * (252 / length(year_positive_excess));
                                            year_info_ratio = round(year_positive_annual_return / year_vol, 2);
                                        else
                                            year_info_ratio = 0;
                                        end
                                    else
                                        year_info_ratio = 0;
                                    end
                                else
                                    year_info_ratio = 0;
                                end
                            else
                                year_info_ratio = 0;
                            end
                            info_ratio_by_year(end+1) = year_info_ratio;
                            
                            trading_days_by_year(end+1) = year_trading_days;
                            
                            fprintf('  %d年: 年化收益率=%.2f%%, 夏普比率=%.2f, 信息比率=%.2f, 最大回撤=%.2f%%, 年化标准差=%.2f%%, 交易日数=%d\n', ...
                                unique_years(y), year_annual_return * 100, year_sharpe, year_info_ratio, ...
                                year_max_dd * 100, year_vol * 100, year_trading_days);
                        end
                    end
                end
                
                % 计算加权平均指标（按交易日数加权）
                if ~isempty(annual_returns_by_year)
                    total_trading_days = sum(trading_days_by_year);
                    if total_trading_days > 0
                        % 使用交易日数加权平均
                        annual_returns2 = sum(annual_returns_by_year .* trading_days_by_year) / total_trading_days;
                        annual_returns = annual_returns2 * 100;
                        
                        % 加权平均年化标准差
                        vol = sum(vol_by_year .* trading_days_by_year) / total_trading_days;
                        
                        % 加权平均夏普比率
                        sharpe = sum(sharpe_by_year .* trading_days_by_year) / total_trading_days;
                        
                        % 加权平均信息比率
                        info_ratio = sum(info_ratio_by_year .* trading_days_by_year) / total_trading_days;
                        
                        % 最大回撤取所有年份的最大值
                        max_dd_all = max(max_dd_by_year);
                        
                        fprintf('加权平均指标 (按交易日数加权):\n');
                        fprintf('  年化收益率: %.2f%%, 夏普比率: %.2f, 信息比率: %.2f, 最大回撤: %.2f%%, 年化标准差: %.2f%%\n', ...
                            annual_returns, sharpe, info_ratio, max_dd_all * 100, vol * 100);
                    else
                        % 如果无法加权，使用简单平均
                        annual_returns2 = mean(annual_returns_by_year);
                        annual_returns = annual_returns2 * 100;
                        vol = mean(vol_by_year);
                        sharpe = mean(sharpe_by_year);
                        info_ratio = mean(info_ratio_by_year);
                        max_dd_all = max(max_dd_by_year);
                        fprintf('平均指标 (简单平均):\n');
                        fprintf('  年化收益率: %.2f%%, 夏普比率: %.2f, 信息比率: %.2f, 最大回撤: %.2f%%, 年化标准差: %.2f%%\n', ...
                            annual_returns, sharpe, info_ratio, max_dd_all * 100, vol * 100);
                    end
                else
                    % 如果无法按年份计算，回退到总体计算
                    excess_cumprod = cumprod(1 + excess_returns_daily);
                    if length(excess_cumprod) > 0
                        total_return = excess_cumprod(end) - 1;
                        % 年化收益率 = 总收益率 * (252 / 实际交易日数)
                        annual_returns2 = total_return * (252 / trading_days);
                        annual_returns = annual_returns2 * 100;
                        fprintf('警告: 无法按年份计算，使用总体指标\n');
                    else
                        annual_returns2 = 0;
                        annual_returns = 0;
                    end
                    % 继续使用总体计算的其他指标
                end
            else
                annual_returns2 = 0;
                annual_returns = 0;
                fprintf('警告: 无法计算年化收益率，超额收益率数据为空或交易日数为0\n');
            end
            
            % 年化标准差（基于超额收益率）
            if ~isempty(excess_returns_daily)
                vol = std(excess_returns_daily) * sqrt(252);
            else
                vol = 0;
            end
            
            % 夏普比率（基于超额收益率）
            if vol > 0
                sharpe = round(annual_returns2 / vol, 2);
            else
                sharpe = 0;
            end
            
            % 信息比率计算（按照新的公式）
            % 筛选出基金和基准收益率都为正的数据点
            if ~isempty(excess_returns_daily) && ~isempty(portfolio_returns) && ~isempty(benchmark_valid)
                % 计算组合收益率和基准收益率
                portfolio_returns_daily = [];
                benchmark_returns_daily = [];
                
                for i = 2:length(portfolio_valid)
                    portfolio_return = (portfolio_valid(i) - portfolio_valid(i-1)) / portfolio_valid(i-1);
                    benchmark_return = (benchmark_valid(i) - benchmark_valid(i-1)) / benchmark_valid(i-1);
                    portfolio_returns_daily(end+1) = portfolio_return;
                    benchmark_returns_daily(end+1) = benchmark_return;
                end
                
                % 筛选出基金和基准收益率都为正的数据点
                positive_mask = (portfolio_returns_daily > 0) & (benchmark_returns_daily > 0);
                if sum(positive_mask) > 0
                    positive_excess_returns = excess_returns_daily(positive_mask);
                    if ~isempty(positive_excess_returns) && vol > 0
                        % 计算信息比率：总收益率 * (252 / 交易日数) / 年化标准差
                        positive_cumprod = cumprod(1 + positive_excess_returns);
                        if length(positive_cumprod) > 0
                            positive_total_return = positive_cumprod(end) - 1;
                            % 年化收益率 = 总收益率 * (252 / 实际交易日数)
                            positive_annual_return = positive_total_return * (252 / length(positive_excess_returns));
                            info_ratio = round(positive_annual_return / vol, 2);
                        else
                            info_ratio = 0;
                        end
                    else
                        info_ratio = 0;
                    end
                else
                    info_ratio = 0;
                end
            else
                info_ratio = 0;
            end
            
            % 最大回撤（基于超额收益率的累积净值）
            if ~isempty(excess_returns_daily)
                excess_nav = cumprod(1 + excess_returns_daily);
                nav_max = cummax(excess_nav);
                drawdowns = (nav_max - excess_nav) ./ nav_max;
                max_dd_all = max(drawdowns);
            else
                max_dd_all = 0;
            end
            
            % 创建业绩指标表格
            performance_metrics = table();
            metric_names = {'Annual_Return_Pct', 'Sharpe_Ratio', 'Info_Ratio', 'Max_Drawdown_Pct', 'Annual_Vol_Pct'};
            metric_values = [annual_returns; sharpe; info_ratio; max_dd_all * 100; vol * 100];
            performance_metrics.Metric = metric_names';
            performance_metrics.Value = metric_values;
            
            % 保存每年的所有指标数据（用于PDF导出）
            % 将每年的数据作为额外的行添加到performance_metrics中
            if exist('annual_returns_by_year', 'var') && ~isempty(annual_returns_by_year) && exist('unique_years', 'var') && ~isempty(unique_years)
                % 为每年添加所有指标的行
                for i = 1:length(unique_years)
                    year_val = unique_years(i);
                    
                    % 年化收益率
                    yearly_metric_name = sprintf('Yearly_Return_%d', year_val);
                    yearly_metric_value = annual_returns_by_year(i) * 100;  % 转换为百分比
                    new_row = table({yearly_metric_name}, yearly_metric_value, ...
                        'VariableNames', {'Metric', 'Value'});
                    performance_metrics = [performance_metrics; new_row];
                    
                    % 夏普比率
                    if exist('sharpe_by_year', 'var') && length(sharpe_by_year) >= i
                        yearly_metric_name = sprintf('Yearly_Sharpe_%d', year_val);
                        yearly_metric_value = sharpe_by_year(i);
                        new_row = table({yearly_metric_name}, yearly_metric_value, ...
                            'VariableNames', {'Metric', 'Value'});
                        performance_metrics = [performance_metrics; new_row];
                    end
                    
                    % 信息比率
                    if exist('info_ratio_by_year', 'var') && length(info_ratio_by_year) >= i
                        yearly_metric_name = sprintf('Yearly_InfoRatio_%d', year_val);
                        yearly_metric_value = info_ratio_by_year(i);
                        new_row = table({yearly_metric_name}, yearly_metric_value, ...
                            'VariableNames', {'Metric', 'Value'});
                        performance_metrics = [performance_metrics; new_row];
                    end
                    
                    % 最大回撤
                    if exist('max_dd_by_year', 'var') && length(max_dd_by_year) >= i
                        yearly_metric_name = sprintf('Yearly_MaxDD_%d', year_val);
                        yearly_metric_value = max_dd_by_year(i) * 100;  % 转换为百分比
                        new_row = table({yearly_metric_name}, yearly_metric_value, ...
                            'VariableNames', {'Metric', 'Value'});
                        performance_metrics = [performance_metrics; new_row];
                    end
                    
                    % 年化标准差
                    if exist('vol_by_year', 'var') && length(vol_by_year) >= i
                        yearly_metric_name = sprintf('Yearly_Vol_%d', year_val);
                        yearly_metric_value = vol_by_year(i) * 100;  % 转换为百分比
                        new_row = table({yearly_metric_name}, yearly_metric_value, ...
                            'VariableNames', {'Metric', 'Value'});
                        performance_metrics = [performance_metrics; new_row];
                    end
                end
            end
            
        end
        
        function exportResults(obj, result_table, performance_metrics, portfolio_name, start_date, end_date, inputpath_backtesting)
            %EXPORTRESULTS 导出结果

         
    
            session_seg = '';
            id_seg = '';
            try
                if ~isempty(obj.portfolio_info) && ismember('portfolio_name', obj.portfolio_info.Properties.VariableNames)
                    row_idx = find(strcmp(obj.portfolio_info.portfolio_name, portfolio_name), 1);
                    if ~isempty(row_idx)
                        % 安全读取 session_id
                        if ismember('session_id', obj.portfolio_info.Properties.VariableNames)
                            sess = obj.portfolio_info.session_id(row_idx);
                            if iscell(sess)
                                sess = sess{1};
                            end
                            session_seg = char(string(sess));
                        end

                        % 安全读取 id
                        if ismember('id', obj.portfolio_info.Properties.VariableNames)
                            idval = obj.portfolio_info.id(row_idx);
                            if iscell(idval)
                                idval = idval{1};
                            end
                            id_seg = char(string(idval));
                        end
                    end
                end
            catch
                
                session_seg = '';
                id_seg = '';
            end

            base_folder = sprintf('%s_回测%s_to_%s', portfolio_name, obj.formatDate(start_date, 'yyyymmdd'), obj.formatDate(end_date, 'yyyymmdd'));

            if ~isempty(session_seg) && ~isempty(id_seg)
                output_dir = fullfile(obj.output_path, obj.user_name, session_seg, id_seg, base_folder);
            elseif ~isempty(session_seg)
                output_dir = fullfile(obj.output_path, obj.user_name, session_seg, base_folder);
            else
                output_dir = fullfile(obj.output_path, obj.user_name, base_folder);
            end
            
            if ~exist(output_dir, 'dir')
                mkdir(output_dir);
            end
            
            % 导出CSV文件
            % 导出时使用不带末尾 _数字 的展示名
            export_base = obj.getExportBaseName(portfolio_name);
            output_filename = sprintf('%s_回测.csv', export_base);
            output_path = fullfile(output_dir, output_filename);
            
            export_table = result_table;
            % 检查表格列数并相应设置列名
            if width(export_table) == 4
                export_table.Properties.VariableNames = {'valuation_date', '基准净值', '组合净值', '超额净值'};
            else
                % 如果列数不匹配，使用默认列名
                fprintf('警告: 表格列数 (%d) 与预期不匹配，使用默认列名\n', width(export_table));
            end
            writetable(export_table, output_path, 'Encoding', 'UTF-8');
            fprintf('CSV文件已导出: %s\n', output_path);
            
            % 导出图表
            obj.exportCharts(result_table, portfolio_name, output_dir);
        end
        
        function exportCharts(obj, result_table, portfolio_name, output_dir)
            %EXPORTCHARTS 导出图表
        
            export_base = obj.getExportBaseName(portfolio_name);
            % 超额净值图
            fig = figure('Position', [100, 100, 1200, 600], 'Visible', 'off');
            set(gcf,'color','w');
            set(gca,'color','w');
            set(gcf,'InvertHardcopy','off');
            
            if isdatetime(result_table.valuation_date)
                dates_for_plot = result_table.valuation_date;
            elseif iscell(result_table.valuation_date)
                dates_for_plot = datetime(result_table.valuation_date);
            else
                dates_for_plot = datetime(obj.formatDate(result_table.valuation_date));
            end
            
            plot(dates_for_plot, result_table.excess_net_value, 'b-', 'LineWidth', 2);
            title(sprintf('%s 超额净值走势图', portfolio_name), 'FontSize', 14, 'FontWeight', 'bold', 'Color', 'k');
            xlabel('日期', 'FontSize', 12, 'Color', 'k');
            ylabel('超额净值', 'FontSize', 12, 'Color', 'k');
            grid on;
            set(gca, 'GridColor', [0.7 0.7 0.7], 'GridLineStyle', '-', 'GridAlpha', 0.6);
            
            ax = gca;
            ax.XAxis.TickLabelFormat = 'yyyy-MM-dd';
            xtickangle(45);
            
            excess_return_fig_path = fullfile(output_dir, sprintf('%s_超额净值图.png', export_base));
            saveas(gcf, excess_return_fig_path);
            close(gcf);
            fprintf('超额净值图已保存: %s\n', excess_return_fig_path);
            
            % 组合基准对比图
            fig = figure('Position', [100, 100, 1200, 600], 'Visible', 'off');
            set(gcf,'color','w');
            set(gca,'color','w');
            set(gcf,'InvertHardcopy','off');
            
            plot(dates_for_plot, result_table.benchmark_net_value, 'r-', 'LineWidth', 2, 'DisplayName', '基准净值');
            hold on;
            plot(dates_for_plot, result_table.portfolio_net_value, 'b-', 'LineWidth', 2, 'DisplayName', '组合净值');
            
            title(sprintf('%s 组合基准对比图', portfolio_name), 'FontSize', 14, 'FontWeight', 'bold', 'Color', 'k');
            xlabel('日期', 'FontSize', 12, 'Color', 'k');
            ylabel('净值', 'FontSize', 12, 'Color', 'k');
            lgd = legend('基准净值','组合净值','Location','best');
            set(lgd, 'Color', 'none', 'TextColor','k');
            grid on;
            set(gca, 'GridColor', [0.7 0.7 0.7], 'GridLineStyle', '-', 'GridAlpha', 0.6);
            
            ax = gca;
            ax.XAxis.TickLabelFormat = 'yyyy-MM-dd';
            xtickangle(45);
            hold off;
            
            comparison_fig_path = fullfile(output_dir, sprintf('%s_组合基准对比图.png', export_base));
            saveas(gcf, comparison_fig_path);
            close(gcf);
            fprintf('组合基准对比图已保存: %s\n', comparison_fig_path);
        end

        function output_dir = getOutputDir(obj, portfolio_name, start_date, end_date)
          
            session_seg = '';
            id_seg = '';
            try
                if ~isempty(obj.portfolio_info) && ismember('portfolio_name', obj.portfolio_info.Properties.VariableNames)
                    row_idx = find(strcmp(obj.portfolio_info.portfolio_name, portfolio_name), 1);
                    if ~isempty(row_idx)
                        if ismember('session_id', obj.portfolio_info.Properties.VariableNames)
                            sess = obj.portfolio_info.session_id(row_idx);
                            if iscell(sess)
                                sess = sess{1};
                            end
                            session_seg = char(string(sess));
                        end
                        if ismember('id', obj.portfolio_info.Properties.VariableNames)
                            idval = obj.portfolio_info.id(row_idx);
                            if iscell(idval)
                                idval = idval{1};
                            end
                            id_seg = char(string(idval));
                        end
                    end
                end
            catch
                session_seg = '';
                id_seg = '';
            end

            base_folder = sprintf('%s_回测%s_to_%s', portfolio_name, obj.formatDate(start_date, 'yyyymmdd'), obj.formatDate(end_date, 'yyyymmdd'));
            if ~isempty(session_seg) && ~isempty(id_seg)
                output_dir = fullfile(obj.output_path, obj.user_name, session_seg, id_seg, base_folder);
            elseif ~isempty(session_seg)
                output_dir = fullfile(obj.output_path, obj.user_name, session_seg, base_folder);
            else
                output_dir = fullfile(obj.output_path, obj.user_name, base_folder);
            end
        end

        function export_base = getExportBaseName(obj, portfolio_name)
            %GETEXPORTBASENAME 返回用于导出的 portfolio 名（去掉末尾的 _<digits>）
            try
                pn = char(string(portfolio_name));
            
                export_base = regexprep(pn, '_\d+$', '');
            catch
                export_base = char(string(portfolio_name));
            end
        end
        
        function calculateContributionAnalysis(obj, portfolio_dates, inputpath_backtesting, df_components, index_type, portfolio_name, start_date, end_date, top_number)
            %CALCULATECONTRIBUTIONANALYSIS 计算成分股贡献分析
            
            
            try
                
                % 获取股票收益率数据
                df_stock_return = BacktestToolbox.stock_return_withdraw(start_date, end_date);
                
                if isempty(df_stock_return)
                    fprintf('警告: 未获取到股票收益率数据，跳过贡献分析\n');
                    return;
                end
                
                
                % 初始化贡献分析结果
                contribution_data = [];
                contribution_dates = [];
                
                % 遍历所有日期计算贡献
                for i = 1:length(portfolio_dates)
                    current_date_str = portfolio_dates(i);
                   
                    fprintf('  处理日期 %d/%d: %s\n', i, length(portfolio_dates), current_date_str);
                    
                    try
                        % 获取当日投资组合权重和评分
                        [daily_weight, daily_scores] = BacktestToolbox.get_portfolio_weights(current_date_str, inputpath_backtesting);
                        
                        
                        if ~isempty(daily_weight) && ~isempty(daily_scores)
                            % 获取当日股票收益率
                            % 确保current_date_str是字符串类型
                            if isdatetime(current_date_str)
                                current_date_str_for_match = string(current_date_str, 'yyyy-MM-dd');
                            else
                                current_date_str_for_match = string(current_date_str);
                            end
                            
                            % 确保日期列是字符串类型并进行匹配
                            if iscell(df_stock_return.valuation_date)
                                % 如果还是cell类型，转换为字符串
                                date_strings = string(df_stock_return.valuation_date);
                                daily_stock_data = df_stock_return(date_strings == current_date_str_for_match, :);
                            else
                                % 直接使用字符串比较
                                daily_stock_data = df_stock_return(df_stock_return.valuation_date == current_date_str_for_match, :);
                            end
                            
                            if ~isempty(daily_stock_data)
                                % 计算贡献
                                contribution_result = BacktestToolbox.calculate_daily_contribution(daily_weight, daily_scores, daily_stock_data, df_components, index_type, top_number);
                                
                                if ~isempty(contribution_result)
                                    contribution_data = [contribution_data; contribution_result];
                                    contribution_dates = [contribution_dates; current_date_str];
                                    
                                else
                                    fprintf('    贡献分析结果为空\n');
                                end
                            end
                        end
                    catch ME
                        fprintf('处理日期 %s 的贡献分析时出错: %s\n', current_date_str, ME.message);
                        continue;
                    end
                end
                
                if ~isempty(contribution_data)
                    % 创建贡献分析表格
                    contribution_table = array2table(contribution_data);
                    contribution_table.Properties.VariableNames = {'missing', 'top', 'component_1_0_0_8', 'component_0_8_0_6', 'component_0_6_0_4', 'component_0_4_0_2', 'component_0_2_0_0'};
                    contribution_table.valuation_date = contribution_dates;
                    
                    % 重新排列列顺序
                    contribution_table = contribution_table(:, [8, 1:7]);
                    
                    % 为了导出时显示正确的列名，创建导出表格
                    export_contribution_table = contribution_table;
                    export_contribution_table.Properties.VariableNames = {'valuation_date', 'missing', 'top', 'component_1.0_0.8', 'component_0.8_0.6', 'component_0.6_0.4', 'component_0.4_0.2', 'component_0.2_0.0'};
                    
                    % 保存贡献分析结果
                    output_dir = obj.getOutputDir(portfolio_name, start_date, end_date);
                    export_base = obj.getExportBaseName(portfolio_name);
                    contribution_csv_path = fullfile(output_dir, sprintf('%s_contribution.csv', export_base));
                    writetable(export_contribution_table, contribution_csv_path, 'Encoding', 'UTF-8');
                    fprintf('成分股贡献分析已保存到: %s\n', contribution_csv_path);
                    
                    % 计算权重贡献分析
                    obj.calculateWeightContributionAnalysis(portfolio_dates, inputpath_backtesting, df_components, top_number, portfolio_name, start_date, end_date);
                    
                else
                    fprintf('警告: 无法计算成分股贡献分析\n');
                end
                
            catch ME
                fprintf('计算成分股贡献分析时出错: %s\n', ME.message);
            end
        end
        
        function calculateWeightContributionAnalysis(obj, portfolio_dates, inputpath_backtesting, df_components, top_number, portfolio_name, start_date, end_date)
            %CALCULATEWEIGHTCONTRIBUTIONANALYSIS 计算权重贡献分析
            
            try
                weight_contribution_data = [];
                weight_contribution_dates = datetime.empty(0,1);
                
                for i = 1:length(portfolio_dates)
                    current_date = portfolio_dates(i);
                    
                    try
                        % 获取当日投资组合权重和评分
                        [daily_weight, daily_scores] = BacktestToolbox.get_portfolio_weights(current_date, inputpath_backtesting);
                        
                        if isempty(daily_weight)
                            continue;
                        end
                        
                        % 计算权重贡献
                        weight_result = BacktestToolbox.calculate_daily_weight_contribution(daily_weight, daily_scores, df_components, top_number);
                        
                        if ~isempty(weight_result)
                            weight_contribution_data = [weight_contribution_data; weight_result];
                            weight_contribution_dates(end+1,1) = current_date;
                        end
                        
                    catch ME
                        fprintf('计算权重贡献分析时出错: %s\n', ME.message);
                        continue;
                    end
                end
                
                % 创建权重贡献分析表格
                if ~isempty(weight_contribution_data)
                    weight_contribution_table = array2table(weight_contribution_data);
                    weight_contribution_table.Properties.VariableNames = {'missing', 'top', 'component_1_0_0_8', 'component_0_8_0_6', 'component_0_6_0_4', 'component_0_4_0_2', 'component_0_2_0_0'};
                    weight_contribution_table.valuation_date = weight_contribution_dates;
                    
                    % 重新排列列顺序
                    weight_contribution_table = weight_contribution_table(:, [8, 1:7]);
                    
                    % 为了导出时显示正确的列名，创建导出表格
                    export_weight_contribution_table = weight_contribution_table;
                    export_weight_contribution_table.Properties.VariableNames = {'valuation_date', 'missing', 'top', 'component_1.0_0.8', 'component_0.8_0.6', 'component_0.6_0.4', 'component_0.4_0.2', 'component_0.2_0.0'};
                    
                    % 保存权重贡献分析结果
                    output_dir = obj.getOutputDir(portfolio_name, start_date, end_date);
                    export_base = obj.getExportBaseName(portfolio_name);
                    weight_contribution_csv_path = fullfile(output_dir, sprintf('%s_contribution_weight.csv', export_base));
                    writetable(export_weight_contribution_table, weight_contribution_csv_path, 'Encoding', 'UTF-8');
                    fprintf('权重贡献分析已保存到: %s\n', weight_contribution_csv_path);
                    
                    % 生成贡献分析图表
                    obj.plotContributionAnalysis(portfolio_name, start_date, end_date, output_dir);
                else
                    fprintf('警告: 无法计算权重贡献分析\n');
                end
                
            catch ME
                fprintf('计算权重贡献分析时出错: %s\n', ME.message);
            end
        end
        
        function generatePDFReport(obj, result_table, performance_metrics, portfolio_name, index_type, start_date, end_date)
            %GENERATEPDFREPORT 生成PDF报告
            output_dir = obj.getOutputDir(portfolio_name, start_date, end_date);
            
            export_base = obj.getExportBaseName(portfolio_name);
            pdf_filename = sprintf('%s回测分析报告_%s_to_%s.pdf', export_base, obj.formatDate(start_date, 'yyyymmdd'), obj.formatDate(end_date, 'yyyymmdd'));
            pdf_path = fullfile(output_dir, pdf_filename);
            
            BacktestToolbox.generate_backtest_pdf(pdf_path, portfolio_name, index_type, start_date, end_date, ...
                result_table, performance_metrics, output_dir);
            
            fprintf('PDF回测分析报告已生成: %s\n', pdf_path);
        end
        
        function plotContributionAnalysis(obj, portfolio_name, start_date, end_date, output_dir)
            %PLOTCONTRIBUTIONANALYSIS 绘制贡献分析图表
            
            try
                % 设置中文字体
                set(0, 'DefaultAxesFontName', 'SimHei');
                set(0, 'DefaultTextFontName', 'SimHei');
                
                % 构建文件路径（使用导出基础名）
                export_base = obj.getExportBaseName(portfolio_name);
                contribution_file = fullfile(output_dir, sprintf('%s_contribution.csv', export_base));
                weight_contribution_file = fullfile(output_dir, sprintf('%s_contribution_weight.csv', export_base));
                
                % 检查文件是否存在
                if ~exist(contribution_file, 'file')
                    fprintf('警告: 成分股贡献分析文件不存在: %s\n', contribution_file);
                    return;
                end
                if ~exist(weight_contribution_file, 'file')
                    fprintf('警告: 权重贡献分析文件不存在: %s\n', weight_contribution_file);
                    return;
                end
                
                % 读取数据
                contribution_data = readtable(contribution_file, 'VariableNamingRule', 'preserve');
                weight_contribution_data = readtable(weight_contribution_file, 'VariableNamingRule', 'preserve');
                
                % 转换日期格式
                contribution_data.valuation_date = datetime(contribution_data.valuation_date);
                weight_contribution_data.valuation_date = datetime(weight_contribution_data.valuation_date);
                
                % 创建图形窗口
                fig = figure('Position', [100, 100, 1200, 800], 'Visible', 'off');
                set(gcf,'color','w');
                set(gca,'color','w');
                
                % 定义列名和对应的中文标题（使用原始列名，包含点号）
                columns = {'missing', 'top', 'component_1.0_0.8', 'component_0.8_0.6', 'component_0.6_0.4', 'component_0.4_0.2', 'component_0.2_0.0'};
                column_names_cn = {'缺失评分', '分数top股票', '成分股1.0-0.8分位', '成分股0.8-0.6分位', '成分股0.6-0.4分位', '成分股0.4-0.2分位', '成分股0.2-0.0分位'};
                
                % 定义颜色
                colors = [0.2 0.4 0.8; 0.8 0.2 0.2; 0.2 0.8 0.2; 0.8 0.6 0.2; 0.6 0.2 0.8; 0.2 0.8 0.8; 0.8 0.4 0.4];
                
                % 绘制成分股贡献分析图
                subplot(2,1,1);
                set(gca,'color','w');
                hold on;
                for i = 1:length(columns)
                    if ismember(columns{i}, contribution_data.Properties.VariableNames)
                        plot(contribution_data.valuation_date, contribution_data.(columns{i}), ...
                            'LineWidth', 2, 'Color', colors(i,:), 'DisplayName', column_names_cn{i});
                    end
                end
                title(sprintf('%s - 成分股贡献分析', portfolio_name), 'FontSize', 14, 'FontWeight', 'bold', 'Color', 'k');
                xlabel('日期', 'FontSize', 12, 'Color', 'k');
                ylabel('贡献度', 'FontSize', 12, 'Color', 'k');
                lgd1 = legend('Location', 'best', 'FontSize', 10);
                set(lgd1, 'Color', 'none', 'TextColor','k');
                grid on;
                set(gca, 'GridColor', [0.7 0.7 0.7], 'GridLineStyle', '-', 'GridAlpha', 0.6);
                hold off;
                
                % 绘制权重贡献分析图
                subplot(2,1,2);
                set(gca,'color','w');
                hold on;
                for i = 1:length(columns)
                    if ismember(columns{i}, weight_contribution_data.Properties.VariableNames)
                        plot(weight_contribution_data.valuation_date, weight_contribution_data.(columns{i}), ...
                            'LineWidth', 2, 'Color', colors(i,:), 'DisplayName', column_names_cn{i});
                    end
                end
                title(sprintf('%s - 权重贡献分析', portfolio_name), 'FontSize', 14, 'FontWeight', 'bold', 'Color', 'k');
                xlabel('日期', 'FontSize', 12, 'Color', 'k');
                ylabel('权重差异', 'FontSize', 12, 'Color', 'k');
                lgd2 = legend('Location', 'best', 'FontSize', 10);
                set(lgd2, 'Color', 'none', 'TextColor','k');
                grid on;
                set(gca, 'GridColor', [0.7 0.7 0.7], 'GridLineStyle', '-', 'GridAlpha', 0.6);
                hold off;
                
                % 调整子图间距
                sgtitle(sprintf('%s 贡献分析对比图', portfolio_name), ...
                    'FontSize', 16, 'FontWeight', 'bold', 'Color', 'k');
                
                % 保存图片
                save_path = fullfile(output_dir, sprintf('%s_贡献分析对比图.png', export_base));
                saveas(gcf, save_path);
                fprintf('贡献分析图表已保存到: %s\n', save_path);
                
            catch ME
                fprintf('生成贡献分析图表时出错: %s\n', ME.message);
            end
        end
    end
    
    methods (Static)
        % 静态方法，包含所有工具函数
        dbc = DatabaseConnector()
        [benchmark_net_value, benchmark_dates] = calculate_benchmark_net_value(dbc, index_type, start_date, end_date, df_components)
        [portfolio_net_value, portfolio_dates, turnover_data, portfolio_returns] = calculate_portfolio_net_value_unified(dbc, index_type, start_date, end_date, data_source, cost_rate)
        [portfolio_weights, portfolio_scores] = get_portfolio_weights(date, inputpath_backtesting)
        turnover_rate = calculate_turnover_rate(prev_weights, curr_weights)
        contribution_result = calculate_daily_contribution(daily_weight, daily_scores, daily_stock_data, df_components, index_type, top_number)
        weight_result = calculate_daily_weight_contribution(daily_weight, daily_scores, df_components, top_number)
        generate_backtest_pdf(pdf_path, portfolio_name, index_type, start_date, end_date, result_table, performance_metrics, output_dir)
        [df_index_return] = index_return_withdraw(start_date, end_date)
        [df_stock_return] = stock_return_withdraw(start_date, end_date)
    end
end
