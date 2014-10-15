/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.plugin.jdbc.pipeline;

import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * A pipeline. This class can iterate over a collection of requests and call an action for a request,
 * synchronously or asynchronously.
 *
 * @param <T> the pipeline result type
 * @param <R> the pipeline request type
 */
public interface Pipeline<T, R extends PipelineRequest> extends Callable<T>, Iterator<R> {

}
