module TablesDatesExt

import Dates
import Tables: Col, colcmp

for T in (Dates.TimeType, Dates.Period)
    @eval begin
        Base.:<(c::Col, v::$T) = colcmp(<, c, v)
        Base.:<(v::$T, c::Col) = colcmp(>, c, v)
        Base.:<=(c::Col, v::$T) = colcmp(<=, c, v)
        Base.:<=(v::$T, c::Col) = colcmp(>=, c, v)
    end
end

end
