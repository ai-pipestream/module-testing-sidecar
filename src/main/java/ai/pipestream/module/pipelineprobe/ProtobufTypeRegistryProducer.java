package ai.pipestream.module.pipelineprobe;

import com.google.protobuf.Descriptors;
import com.google.protobuf.TypeRegistry;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Produces a protobuf {@link TypeRegistry} populated from generated Java classes.
 * This enables {@link com.google.protobuf.util.JsonFormat.Printer} to resolve
 * {@code google.protobuf.Any} fields (e.g. TikaResponse packed inside PipeDoc).
 *
 * When the shared pipestream extension provides this bean natively, delete this class.
 */
@Singleton
public class ProtobufTypeRegistryProducer {

    private static final Logger LOG = Logger.getLogger(ProtobufTypeRegistryProducer.class);

    @Produces
    @Singleton
    public TypeRegistry typeRegistry() {
        Map<String, Descriptors.FileDescriptor> fileDescriptors = new LinkedHashMap<>();

        Descriptors.FileDescriptor[] seeds = {
                ai.pipestream.data.v1.PipeDoc.getDescriptor().getFile(),
                ai.pipestream.data.v1.ParsedMetadata.getDescriptor().getFile(),
                ai.pipestream.data.v1.SearchMetadata.getDescriptor().getFile(),
                ai.pipestream.data.v1.Blob.getDescriptor().getFile(),
                ai.pipestream.data.v1.BlobBag.getDescriptor().getFile(),
                ai.pipestream.data.module.v1.ProcessDataResponse.getDescriptor().getFile(),
                ai.pipestream.testing.harness.v1.RunModuleTestResponse.getDescriptor().getFile(),
                ai.pipestream.parsed.data.tika.v1.TikaResponse.getDescriptor().getFile(),
                ai.pipestream.parsed.data.generic.v1.GenericMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.docling.v1.DoclingResponse.getDescriptor().getFile(),
                ai.pipestream.parsed.data.pdf.v1.PdfMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.html.v1.HtmlMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.office.v1.OfficeMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.image.v1.ImageMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.media.v1.MediaMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.email.v1.EmailMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.epub.v1.EpubMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.warc.v1.WarcMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.database.v1.DatabaseMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.climate.v1.ClimateForcastMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.rtf.v1.RtfMetadata.getDescriptor().getFile(),
                ai.pipestream.parsed.data.creative_commons.v1.CreativeCommonsMetadata.getDescriptor().getFile(),
        };

        for (Descriptors.FileDescriptor fd : seeds) {
            collectWithDependencies(fd, fileDescriptors);
        }

        TypeRegistry.Builder builder = TypeRegistry.newBuilder();
        int count = 0;
        for (Descriptors.FileDescriptor fd : fileDescriptors.values()) {
            for (Descriptors.Descriptor msgType : fd.getMessageTypes()) {
                registerRecursively(builder, msgType);
                count++;
            }
        }

        LOG.infof("Protobuf TypeRegistry built with %d top-level message types from %d file descriptors",
                count, fileDescriptors.size());
        return builder.build();
    }

    private void collectWithDependencies(Descriptors.FileDescriptor fd,
                                         Map<String, Descriptors.FileDescriptor> seen) {
        if (seen.containsKey(fd.getFullName())) return;
        seen.put(fd.getFullName(), fd);
        for (Descriptors.FileDescriptor dep : fd.getDependencies()) {
            collectWithDependencies(dep, seen);
        }
    }

    private void registerRecursively(TypeRegistry.Builder builder, Descriptors.Descriptor descriptor) {
        try {
            builder.add(descriptor);
        } catch (IllegalArgumentException e) {
            // duplicate type, harmless
        }
        for (Descriptors.Descriptor nested : descriptor.getNestedTypes()) {
            registerRecursively(builder, nested);
        }
    }
}
