class TARConf:
    def __init__(self, name, minsup, minconf, dataset, max_pattern_size, epsilon, min_overlap, output_path, save_patterns=False):
        self.Name = name
        self.MinSupport = minsup
        self.MinConfidence = minconf
        self.Dataset = dataset
        self.ResultsDir = output_path
        self.MaxPatternSize = max_pattern_size
        self.SavePatterns = save_patterns
        self.Epsilon = epsilon
        self.MinOverlap = min_overlap
