﻿using System.Collections.Generic;
using DataTools.Sync.Model.Schema;

namespace DataTools.Sync.Model.Configuration
{
    public class SynchronizationSet
    {
        public string Name { get; set; }
        public bool IsDisabled { get; set; }
        public bool IsDryRun { get; set; }
        public int BufferSize { get; set; }
        public int ConcurrentWorkers { get; set; }
        public IList<Variable> Variables { get; set; }
        public Source Source { get; set; }
        public Destination Destination { get; set; }
        public IList<Table> Tables { get; set; }
        public Dictionary<string, Validator> Validators { get; set; }

        public DatabaseSchema SourceDatabase { get; set; }
        public DatabaseSchema DestinationDatabase { get; set; }

        public Validator GetValidator(string key)
        {
            Validator validator = null;
            Validators?.TryGetValue(key, out validator);
            return validator;
        }
    }
}