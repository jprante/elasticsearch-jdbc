
package org.xbib.pipeline;

public interface PipelineRequest<E> {

    E get();

    PipelineRequest<E> set(E e);

}
