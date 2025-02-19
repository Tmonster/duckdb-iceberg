
class DataGenerationBase:
    """Base class for database connections and operations."""
    def GetConnection(self):
        """Returns a connection to the database."""
        raise NotImplementedError("Subclasses must implement GetConnection()")

    def SetupData(self):
        """Sets up data before table generation."""
        raise NotImplementedError("Subclasses must implement SetupData()")

    def GenerateTables(self, con):
        """Creates tables using the provided database connection."""
        raise NotImplementedError("Subclasses must implement GenerateTables()")

    def CloseConnection(self, con):
        raise NotImplementedError("Subclasses must implement CloseConnection()")