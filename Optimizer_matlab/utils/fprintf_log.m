function varargout = fprintf_log(fmt, varargin)


% Call builtin fprintf so we don't recurse
try
    [varargout{1:nargout}] = builtin('fprintf', fmt, varargin{:});
catch
    % If builtin fprintf fails for any reason, still try to format and log
    try
        msg = sprintf(fmt, varargin{:});
        builtin('fprintf', '%s', msg);
    catch
        % swallow
    end
    if nargout>0
        varargout = cell(1, nargout);
    end
end

% Also append to repo log (non-blocking, swallow errors)
try
    % Format message for file logging (ensure newline)
    try
        msg = sprintf(fmt, varargin{:});
    catch
        try
            msg = char(fmt);
        catch
            msg = '<unformattable log message>';
        end
    end
    if isempty(msg) || msg(end) ~= char(10)
        msg = [msg char(10)];
    end
    append_log_to_file(msg);
catch
    % ignore logging failures
end
end
