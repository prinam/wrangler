/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.codec;

import co.cask.wrangler.api.Decoder;
import co.cask.wrangler.api.Record;
import com.example.tutorial.AddressBookProtos;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

/**
 * Tests {@link ProtobufDecoderUsingDescriptor}
 */
public class ProtobufDecoderUsingDescriptorTest {

  @Test
  public void testBasicConversion() throws Exception {
    AddressBookProtos.Person john = AddressBookProtos.Person.newBuilder()
      .setId(1234)
      .setName("Joltie Root")
      .setEmail("joltie.root@example.com")
      .addPhones(
        AddressBookProtos.Person.PhoneNumber.newBuilder()
          .setNumber("555-4321")
          .setType(AddressBookProtos.Person.PhoneType.HOME)
      ).build();

    AddressBookProtos.AddressBook book = AddressBookProtos.AddressBook.newBuilder()
      .addPeople(john).build();

    byte[] addressBook = book.toByteArray();

    InputStream is = null;
    try {
      is = this.getClass().getClassLoader().getResourceAsStream("addressbook.desc");
      byte[] bytes = IOUtils.toByteArray(is);
      Decoder<Record> decoder = new ProtobufDecoderUsingDescriptor(bytes, "AddressBook");
      Assert.assertNotNull(decoder);
      List<Record> records = decoder.decode(addressBook);
      Assert.assertNotNull(records);
      Assert.assertEquals("Joltie Root", records.get(0).getValue("people_name"));
      Assert.assertEquals(1234, records.get(0).getValue("people_id"));
      Assert.assertEquals("joltie.root@example.com", records.get(0).getValue("people_email"));
      Assert.assertEquals("555-4321", records.get(0).getValue("people_phones_number"));
      Assert.assertEquals("HOME", records.get(0).getValue("people_phones_type"));
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
}
