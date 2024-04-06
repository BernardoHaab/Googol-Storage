package googol;

import jakarta.persistence.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.query.SelectionQuery;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class StorageBarrel extends Thread {
    private String HOST_NAME;
    private int PORT;
    private int PORT_RETRIEVE;

    EntityManagerFactory entityManagerFactory;
    EntityManager em;

    private MulticastSocket socket = null;
    private NetworkInterface networkInterface;
    private InetAddress mcastaddr;
    private UUID storageId;
    private int syncId;

    private boolean isReady = false;

    // Palavra -> Lista de URLs
    private ConcurrentHashMap<String, HashSet<String>> storage = new ConcurrentHashMap<String, HashSet<String>>();

    // URL -> Lista de URLs que referenciam a URL
    private ConcurrentHashMap<String, HashSet<String>> urls = new ConcurrentHashMap<String, HashSet<String>>();

    // senderId -> conjunto de messageId
    private HashMap<UUID, Integer> lastMessages = new HashMap<UUID, Integer>();

    // senderId -> conjunto de messageId
    private HashMap<UUID, Integer> retrievingMessages = new HashMap<UUID, Integer>();
    private HashMap<UUID, Integer> attemptedRetrives = new HashMap<UUID, Integer>();

    // Buffer de mensagens aguardando para serem computadas
    // senderId -> (messageId -> Req)
    private HashMap<UUID, HashMap<Integer, Req>> messageBuffer = new HashMap<UUID, HashMap<Integer, Req>>();

    public StorageBarrel(String hostName, int port, int portRetrieve, EntityManager em) {
        this.HOST_NAME = hostName;
        this.PORT = port;
        this.PORT_RETRIEVE = portRetrieve;
        this.storageId = UUID.randomUUID();
        this.syncId = 0;

        this.em = em;

    }

    @Override
    public void run() {
        System.out.println("Storage Barrel " + super.getId() + " running");

        try {
            socket = new MulticastSocket(PORT);
            networkInterface = NetworkInterface.getByIndex(0);
            mcastaddr = InetAddress.getByName(HOST_NAME);

            socket.joinGroup(new InetSocketAddress(mcastaddr, 0), networkInterface);

            isReady = true;
            byte[] buffer = new byte[1024];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                System.out.println("\n\n");
                System.out.println("Waiting for a message from the multicast group...");
                System.out.println("Address: " + packet.getAddress().getHostAddress());
                System.out.println("Address: " + packet.getSocketAddress());
                System.out.println("Port: " + packet.getPort());

                Req req = parseMessage(new String(buffer));
                processReq(req);
                buffer = new byte[1024];

                System.out.println("\t\t --> Tamanho do Index: " + storage.size());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Req parseMessage(String message) {
        try {
            return new Req(message);
        } catch (Exception e) {
            System.out.println("Error parsing message");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Se recebe a mensagem esperada, computa ela, incrementa o expectedMessageId e
     * ve se a próxima esta no buffer
     * - Se esta processa ela da mesma forma (Chama recursivamente o processMessage)
     * Se recebe mensagem com id menor que o esperado, ignora
     * Se recebe mensagem com id maior que o esperado, coloca ela no buffer e cria
     * timeout para enviar pedido de reenvio
     * - Quando timeout estourar, Se o expectedMessageId for < que o id da mensagem
     * no buffer, envia pedido de reenvio da expectedMessageId
     */
    private void processReq(Req req) {
        System.out.println("--->Processing message");

        LastMessage lastMessage = getLastMessage(req.getSenderId());
        int expectedMessageId = lastMessage.getLastMessageId() + 1;
        boolean isNewMessage = req.getMessageId() >= expectedMessageId;

        if (!isNewMessage) {
            System.out.println("-->Message already received");
            return;
        }

        if (req.getMessageId() != expectedMessageId && isReady) {
            System.out.println("-->Unexpected message id");
            System.out.println("--->Expected: " + expectedMessageId);
            System.out.println("--->Received: " + req.getMessageId());
            addMessageToBuffer(req);

            Integer retrievingMessageId = retrievingMessages.get(req.getSenderId());

            if (retrievingMessageId == null || retrievingMessageId != expectedMessageId) {
                retrievingMessages.put(req.getSenderId(), expectedMessageId);
                requestRetrieve(req, expectedMessageId);
            }
            return;
        }
        expectedMessageId++;

        try {
            if (isReady) {
                switch (req.getType()) {
                    case "WORD_LIST":
                        updateStorage(req.getContent());

                        System.out.println("---------------------Storage updated---------------------");
                        break;

                    case "REFERENCED_URLS":
                        addReferencedUrls(req.getContent());

                        System.out.println("---------------------Referenced URLs updated---------------------");
                        break;
                    case "REQ_SYNC":
                        System.out.println("---------------------RECEBEU PEDIDO DE RESYNC---------------------");
                        ConcurrentHashMap<String, HashSet<String>> storageCopy = new ConcurrentHashMap<String, HashSet<String>>();
                        ConcurrentHashMap<String, HashSet<String>> urlsCopy = new ConcurrentHashMap<String, HashSet<String>>();
                        HashMap<UUID, Integer> lastMessagesCopy = new HashMap<UUID, Integer>();

                        for (String word : storage.keySet()) {
                            HashSet<String> wordUrls = storage.get(word);
                            storageCopy.put(word, wordUrls);
                        }

                        for (String url : urls.keySet()) {
                            HashSet<String> page = urls.get(url);
                            urlsCopy.put(url, page);
                        }

                        for (UUID senderId : lastMessages.keySet()) {
                            Integer lastMessageId = lastMessages.get(senderId);
                            lastMessagesCopy.put(senderId, lastMessageId);
                        }

//                        new Sync(HOST_NAME, PORT, storageCopy, urlsCopy, lastMessagesCopy, storageId);

                        break;
                    default:
                        System.out.println("Invalid message type");
                        System.out.println("Type: " + req.getType());
                        break;
                }
            }


            Session session = em.unwrap(Session.class);

            try {
                session.beginTransaction();
                session.saveOrUpdate(lastMessage);
                lastMessage.setLastMessageId(req.getMessageId());
            } catch (PersistenceException e) {
                System.out.println("SENDER JÁ EXISTE");
            } catch (Exception e) {
                System.out.println("Error persisting last message");
                e.printStackTrace();
            } finally {
                session.getTransaction().commit();
            }


            if (messageBuffer.containsKey(req.getSenderId())) {
                HashMap<Integer, Req> buffer = messageBuffer.get(req.getSenderId());
                if (buffer.containsKey(expectedMessageId)) {

                    Req nextMessage = buffer.get(expectedMessageId);
                    buffer.remove(expectedMessageId);
                    processReq(nextMessage);
                }
            }

        } catch (Exception e) {
            System.out.println("Error parsing message");
            e.printStackTrace();
        }
    }

    private LastMessage getLastMessage(UUID senderId) {
        Session session = em.unwrap(Session.class);
        LastMessage lastMessage;
        try {
//            session.beginTransaction();
            TypedQuery<LastMessage> q = session.createNamedQuery("LastMessage.bySenderId", LastMessage.class);
            q.setParameter("senderId", senderId);
            lastMessage = q.getSingleResult();
        } catch (NoResultException e) {
            lastMessage = new LastMessage(senderId);
        } catch (Exception e) {
            System.out.println("Error retrieving last message");
            e.printStackTrace();
            lastMessage = new LastMessage(senderId);
        }
        System.out.println(lastMessage);
        return lastMessage;
    }

    private void addMessageToBuffer(Req req) {
        UUID senderId = req.getSenderId();
        int messageId = req.getMessageId();

        if (messageBuffer.containsKey(senderId)) {
            HashMap<Integer, Req> buffer = messageBuffer.get(senderId);
            buffer.put(messageId, req);
        } else {
            HashMap<Integer, Req> buffer = new HashMap<Integer, Req>();
            buffer.put(messageId, req);
            messageBuffer.put(senderId, buffer);
        }
    }

    private void requestRetrieve(Req req, int messageToRetrieve) {
        new Timer().schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        // Verifica se a mensagem esperada foi recebida durante o intervalo
                        Integer lastMessage = 0;

                        if (lastMessages.containsKey(req.getSenderId())) {
                            lastMessage = lastMessages.get(req.getSenderId());
                        }

                        if (messageToRetrieve < lastMessage) {
                            return;
                        }

                        Integer attempts = attemptedRetrives.get(req.getSenderId());

                        if (attempts == null) {
                            attempts = 0;
                        }

                        System.out.println("----------------------------------------------------------------------");
                        System.out.println("Attempts: " + attempts);

                        if (attempts >= 3) {
                            try {
                                System.out.println("===================>Syncing with other storages");
                                String message = storageId + "|" + syncId + ";TYPE|REQ_SYNC;";
                                byte[] buffer = message.getBytes();
                                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, mcastaddr, PORT);
                                socket.send(packet);
                                isReady = false;
                                return;
                            } catch (Exception e) {
                                System.out.println("Error requesting sync");
                            }
                        } else {
                            attempts++;
                            attemptedRetrives.put(req.getSenderId(), attempts);
                        }

                        // Se não foi, pede ela
                        try {
                            System.out.println("Retrieving message: " + messageToRetrieve);
                            String message = "TYPE|RETRIEVE; " + req.getSenderId() + "|" +
                                    messageToRetrieve;
                            sendRetrieveMessage(message);
                        } catch (Exception e) {
                            System.out.println("Error sending request");
                        } finally {
                            // Enquanto não receber a mensagem esperada, fica pedindo ela
                            requestRetrieve(req, messageToRetrieve);
                        }
                    }
                },
                200);
    }

    private void updateStorage(String[] message) {
        List<String> newWords = new LinkedList<>();

        String url = message[0].trim().split("\\|")[1];


        String[] items = new String[message.length - 2];
        System.arraycopy(message, 2, items, 0, message.length - 2);

        for (String item : items) {
            String[] wordContent = item.trim().split("\\|");
            String word = wordContent[1];


            newWords.add(word);
        }

        addWords(url, newWords);
//        addWords(url, newWords);
    }

    private void addWords(String url, List<String> words) {
        //Get Session
        Session session = em.unwrap(Session.class);
        Page currentPage = Page.getPageByUrl(url, session);

        try {
            session.beginTransaction();
            //Save the Model object

            for (String word : words) {
                WordIndex wordIndex = new WordIndex(word);
                wordIndex.addPage(currentPage);

                try {
                    TypedQuery<WordIndex> q = session.createNamedQuery("WordIndex.word", WordIndex.class);
                    q.setParameter("word", word);
                    wordIndex = q.getSingleResult();
                    System.out.println(wordIndex);
                    wordIndex.addPage(currentPage);
                    session.persist(wordIndex);
                } catch (NoResultException e) {
                    session.persist(wordIndex);
                } catch (Exception e) {
                    System.out.println("Error adding new word");
                    e.printStackTrace();
                }

            }

        } catch (PersistenceException e) {
            System.out.println("Word already exists");
        } catch (Exception e) {
//            session.getTransaction().rollback();
            System.out.println("Error adding words");
            e.printStackTrace();
        } finally {
            session.getTransaction().commit();
        }
    }

    private void addReferencedUrls(String[] message) {
        String url = message[0].trim().split("\\|")[1];

        String[] items = new String[message.length - 2];
        System.arraycopy(message, 2, items, 0, message.length - 2);

        Session session = em.unwrap(Session.class);
        try {
            Page page = Page.getPageByUrl(url, session);
            session.persist(page);

            for (String item : items) {
                String[] linkContent = item.trim().split("\\|");
                String link = linkContent[1];

                page.addReference(Page.getPageByUrl(link, session));

                HashSet<String> referencedBy = urls.get(link);

                if (referencedBy == null) {
                    referencedBy = new HashSet<String>();
                    referencedBy.add(url);
                    urls.put(link, referencedBy);
                } else {
                    referencedBy.add(url);
                }
            }

            session.beginTransaction();

            System.out.println(page);
            session.persist(page);
        } catch (PersistenceException e) {
            System.out.println("Reference already exists");
        } catch (Exception e) {
            System.out.println("Error adding referenced urls");
            e.printStackTrace();
        } finally {
            session.getTransaction().commit();
        }
    }

    private void printStorage() {
        System.out.println("-----START - Printing storage-----");
        Iterator<String> it = storage.keySet().iterator();
        while (it.hasNext()) {
            String word = it.next();
            HashSet<String> urls = storage.get(word);

            System.out.println("Word: " + word);
            System.out.println("URLs: " + urls.toString());
        }
        System.out.println("-----END - Printing storage-----");
    }

    private void printUrls() {
        Iterator<String> it = urls.keySet().iterator();
        while (it.hasNext()) {
            String url = it.next();
            HashSet<String> referencedBy = urls.get(url);

            System.out.println("URL: " + url);
            System.out.println("Referenced by: " + referencedBy.toString());
        }
    }

    private void sendRetrieveMessage(String message) throws IOException {
        System.out.println("Sending retrieve message: " + message);
        byte[] buffer = message.getBytes();
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length, mcastaddr, PORT_RETRIEVE);
        socket.send(packet);
    }

    public List<PageDTO> searchByTerms(Set<String> terms, int page) {
        List<PageDTO> pages = new LinkedList<>();

        Query q = em.createNativeQuery("Select p.PAGE_ID , p.URL, p.TITLE, p.QUOTE, (select count(pp.referencedBy_PAGE_ID) from page_page pp \n" +
                "join page p1 on p1.PAGE_ID = pp.referencePages_PAGE_ID\n" +
                "where p.URL = p1.URL) as recerencedBy  \n" +
                "FROM word_page wp\n" +
                "inner join page p on p.PAGE_ID = wp.PAGE_ID\n" +
                "inner join wordindex w on w.INDEX_ID = wp.INDEX_ID\n" +
                "WHERE w.WORD IN :terms\n" +
                "GROUP BY wp.PAGE_ID \n" +
                "HAVING COUNT(*) = :qnt_terms\n" +
                "order by recerencedBy desc\n" +
                "limit 10\n" +
                "offset :offset");

        q.setParameter("terms", terms);
        q.setParameter("qnt_terms", terms.size());
        q.setParameter("offset", page*10);

        List<Object[]>  resultList = q.getResultList();

        for (Object[] row : resultList) {
            PageDTO p = new PageDTO();
            p.setId((int) row[0]);
            p.setUrl((String) row[1]);
            p.setTitle((String) row[2]);
            p.setQuote((String) row[3]);
            pages.add(p);
        }

        return pages;
    }



}

