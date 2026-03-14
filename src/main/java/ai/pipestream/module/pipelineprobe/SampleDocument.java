package ai.pipestream.module.pipelineprobe;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Curated set of well-known sample documents bundled into the sidecar JAR
 * for quick testing without needing a repository or manual upload.
 */
public enum SampleDocument {

    ALICE_IN_WONDERLAND("Alice in Wonderland", "alice_in_wonderland.txt",
            170_597, "text/plain", "Lewis Carroll's classic novel"),

    PRIDE_AND_PREJUDICE("Pride and Prejudice", "pride_and_prejudice.txt",
            757_512, "text/plain", "Jane Austen's novel"),

    US_CONSTITUTION("U.S. Constitution", "constitution.txt",
            47_541, "text/plain", "Full text of the United States Constitution"),

    SAMPLE_ARTICLE("Sample Article", "sample_article.txt",
            2_211, "text/plain", "Short news-style article"),

    CATH_AND_BRAZZ("Cath and Brazz", "cath_and_brazz.txt",
            106_691, "text/plain", "Short story"),

    ATTENTION_PAPER("Attention Is All You Need", "attn_all_you_need_1706.03762v7.pdf",
            2_215_244, "application/pdf", "Vaswani et al. 2017 transformer paper"),

    APPLE_FINANCIALS("Apple FY25 Q4 Financials", "apple_FY25_Q4_Consolidated_Financial_Statements.pdf",
            4_919_649, "application/pdf", "Apple quarterly financial statements"),

    IRS_1040("IRS Form 1040", "irs_f1040.pdf",
            220_237, "application/pdf", "U.S. individual income tax return form");

    private static final String RESOURCE_DIR = "sample-data/";

    private final String title;
    private final String fileName;
    private final long sizeBytes;
    private final String mimeType;
    private final String description;

    SampleDocument(String title, String fileName, long sizeBytes, String mimeType, String description) {
        this.title = title;
        this.fileName = fileName;
        this.sizeBytes = sizeBytes;
        this.mimeType = mimeType;
        this.description = description;
    }

    public String getTitle() { return title; }
    public String getFileName() { return fileName; }
    public long getSizeBytes() { return sizeBytes; }
    public String getMimeType() { return mimeType; }
    public String getDescription() { return description; }
    public String getResourcePath() { return RESOURCE_DIR + fileName; }

    public byte[] loadBytes() throws IOException {
        try (InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(getResourcePath())) {
            if (is == null) {
                throw new IOException("Sample file not found on classpath: " + getResourcePath());
            }
            return is.readAllBytes();
        }
    }

    public Map<String, Object> toInfo() {
        return Map.of(
                "id", name(),
                "title", title,
                "fileName", fileName,
                "sizeBytes", sizeBytes,
                "mimeType", mimeType,
                "description", description
        );
    }

    public static SampleDocument fromId(String id) {
        try {
            return valueOf(id);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown sample document: " + id
                    + ". Valid IDs: " + java.util.Arrays.toString(
                    java.util.Arrays.stream(values()).map(Enum::name).toArray()));
        }
    }
}
