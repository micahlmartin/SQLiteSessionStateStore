SQLiteSessionStateProvider
========================================
This is a SQLite session state provider for ASP.NET. There are 3 providers that come standard: InProc, SqlServer, and StateServer.
The SQLite provider was built to address deployment scenarious where persistant session state needs to be stored but SQLServer
and StateServer are either not available or performance is a concern.



Dependencies
========================================
This project requires the System.Data.SQLite.dll from http://sqlite.phxsoftware.com/. Both the x86 and x64 version are included
with the source.



Example
========================================
The provider will handle setting up the schema and creating the database automatically.
Just add the following configuration to your web.config file

<system.web>
	<sessionState mode="Custom" customProvider="Sqlite">
	  <providers>
		<add name="Sqlite" type="Littlefish.SQLiteSessionStateProvider.SQLiteSessionStateStoreProvider, Littlefish.SQLiteSessionStateProvider" databaseFile="~/App_Data/SessionState.db3" />
	  </providers>
	</sessionState>
</system.web>

