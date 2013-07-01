//------------------------------------------------------------------------------
// <copyright file="CSSqlStoredProcedure.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using Edge.Core.Utilities;

public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static SqlInt32 SessionDecryptor(string session)
    {
        string sessionId = string.Empty;
        
        string KeyEncrypt = "5c51374e366f41297356413c71677220386c534c394742234947567840";
       // string KeyEncrypt = "8666094D1EC7452A4B209A9360233E2BCA35380216BE3037A375DEA50CFFD9B4";
      
        Encryptor encryptor = new Encryptor(KeyEncrypt);
        try
        {
            sessionId = encryptor.Decrypt(session);
        }
        catch (Exception e)
        {
            throw new Exception("Error SessionDecryptor Stored Procedure: Invalid Session,session could no be parse!   "+e.Message);
        }

        return (SqlInt32)Convert.ToInt32(sessionId);

    }
}
