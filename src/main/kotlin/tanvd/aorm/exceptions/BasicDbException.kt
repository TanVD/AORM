package tanvd.aorm.exceptions

import java.sql.SQLException

class BasicDbException(msg: String, cause: Exception) : SQLException(msg, cause)