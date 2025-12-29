function [benchmark_net_value, benchmark_dates] = calculate_benchmark_net_value(dbc, index_type, start_date, end_date, df_components)
    % 计算基准净值
    % 输入:
    %   dbc - 数据库连接器
    %   index_type - 指数类型 (如 '沪深300', '中证500' 等)
    %   start_date - 开始日期
    %   end_date - 结束日期
    %   df_components - 指数成分股信息
    % 输出:
    %   benchmark_net_value - 基准净值序列
    %   benchmark_dates - 对应的日期序列
    
    fprintf('开始计算基准净值...\n');
    fprintf('指数类型: %s\n', index_type);
    fprintf('成分股数量: %d\n', height(df_components));
    
    % 获取指数收益率数据
    df_index_return = BacktestToolbox.index_return_withdraw(start_date, end_date);
    
    if isempty(df_index_return)
        error('未获取到指数收益率数据');
    end
    
    % 将指数类型转换为数据库代码
    switch index_type
        case {'沪深300', 'hs300'}
            index_code = '000300.SH';  % 沪深300指数代码
        case {'中证500', 'zz500'}
            index_code = '000905.SH';  % 中证500指数代码
        case {'中证1000', 'zz1000'}
            index_code = '000852.SH';  % 中证1000指数代码
        case {'中证2000', 'zz2000'}
            index_code = '932000.CSI'; % 中证2000指数代码
        case {'中证A500', 'zzA500'}
            index_code = '000510.CSI'; % 中证A500指数代码
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
    
    if isempty(index_data)
        fprintf('未找到指数 %s (代码: %s) 的收益率数据\n', index_type, index_code);
        fprintf('可用的指数代码: %s\n', strjoin(unique(df_index_return.code), ', '));
        error('未找到指数 %s 的收益率数据', index_type);
    end
    
    % 按日期排序
    index_data = sortrows(index_data, 'valuation_date');
    
    % 提取日期和收益率
    benchmark_dates = index_data.valuation_date;
    index_returns = index_data.pct_chg;
    
    % 计算基准净值 - 逐日计算并打印过程
    fprintf('\n=== 开始逐日计算基准净值 ===\n');
    fprintf('获取到 %d 个交易日的数据\n', length(benchmark_dates));
    
    % 处理日期格式
    for i = 1:numel(benchmark_dates)
        d = benchmark_dates(i);
        % 判断是不是包含字母月份 (说明是 dd-MMM-yyyy 格式)
        if contains(d, {'Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'})
            dt = datetime(d, 'InputFormat', 'dd-MMM-yyyy', 'Locale', 'en_US');
            benchmark_dates(i) = string(dt, 'yyyy-MM-dd');
        end
    end
    
    % 使用cumprod计算基准净值，与Python版本完全一致
    benchmark_net_value = cumprod(1 + index_returns);

    fprintf('第1天: %s\n', char(benchmark_dates(1)));
    fprintf('  净值计算: %g = cumprod(1 + %g)\n', benchmark_net_value(1), index_returns(1));
    fprintf('  当日净值: %g\n\n', benchmark_net_value(1));
    
    % 显示前几天的计算结果
    for i = 2:min(5, length(index_returns))
        fprintf('第%d天: %s\n', i, char(benchmark_dates(i)));
        fprintf('  当日净值: %f\n', benchmark_net_value(i));
    end
    
    % 如果有很多数据，显示最后几天（仅用于验证，不重新计算）
    if length(index_returns) > 15
        start_idx = length(index_returns) - 4;
        for i = start_idx:length(index_returns)
            if i > 10  % 避免重复打印
                
                % 验证计算：应该等于前一日净值 × (1 + 当日收益率)
                expected_value = benchmark_net_value(i-1) * (1 + index_returns(i));
                
                fprintf('第%d天: %s\n', i, char(benchmark_dates(i)));
                fprintf('  收益率: %g (%g%%)\n', index_returns(i), index_returns(i)*100);
                fprintf('  净值计算: %g = %g × (1 + %g)\n', ...
                        benchmark_net_value(i), benchmark_net_value(i-1), index_returns(i));
                fprintf('  当日净值: %g\n', benchmark_net_value(i));
                
                % 验证计算是否正确（允许微小的浮点数误差）
                if abs(benchmark_net_value(i) - expected_value) > 1e-10
                    fprintf('  警告：净值计算不一致！cumprod结果=%g, 逐日计算=%g, 差异=%g\n', ...
                        benchmark_net_value(i), expected_value, abs(benchmark_net_value(i) - expected_value));
                end
                
                % 计算相对第一天的累计收益率
                cumulative_return = (benchmark_net_value(i) - 1) * 100;
                fprintf('  累计收益: %g%%\n\n', cumulative_return);
            end
        end
    end
    
    fprintf('=== 净值计算完成 ===\n');
    fprintf('起始净值: %g (基准日期: %s)\n', benchmark_net_value(1), char(benchmark_dates(1)));
    fprintf('最终净值: %g (结束日期: %s)\n', benchmark_net_value(end), char(benchmark_dates(end)));
    fprintf('总收益率: %g%%\n', (benchmark_net_value(end) - 1) * 100);
    
    % 创建结果表格
    result_table = table(benchmark_dates, benchmark_net_value, ...
                        'VariableNames', {'valuation_date', 'benchmark_net_value'});
    
    % 保存结果到工作空间
    assignin('base', 'benchmark_result', result_table);
    
    fprintf('基准净值计算完成，结果已保存到 benchmark_result 变量\n');
end
