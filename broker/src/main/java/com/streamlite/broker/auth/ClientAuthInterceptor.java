package com.streamlite.broker.auth;

import com.streamlite.broker.user.exception.UserNotFoundException;
import com.streamlite.broker.user.model.TopicUser;
import com.streamlite.broker.user.repository.TopicUserRepository;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class ClientAuthInterceptor implements HandlerInterceptor {

    private final TopicUserRepository topicUserRepository;

    public ClientAuthInterceptor(
            @Autowired
            TopicUserRepository topicUserRepository) {
        this.topicUserRepository = topicUserRepository;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        String tenantId = request.getHeader("X-Tenant-Id");
        String timestamp = request.getHeader("X-Timestamp");
        String signature = request.getHeader("X-Signature");

        if (tenantId == null || timestamp == null || signature == null) {
            response.sendError(401, "Missing authentication headers");
            return false;
        }

        Optional<TopicUser> topicUser = topicUserRepository.findByTenantId(tenantId);
        if(topicUser.isEmpty())
            throw new UserNotFoundException("User " + tenantId + " not found!");

        PublicKey publicKey = PemUtils.parsePublicKey(topicUser.get().getPublicKeyPem());

        String canonical = buildCanonicalString(request, timestamp, tenantId);

        if (!verifySignature(publicKey, canonical, signature)) {
            response.sendError(401, "Invalid signature");
            return false;
        }

        return true;
    }

    private String buildCanonicalString(HttpServletRequest req, String timestamp, String clientId) throws Exception {

        String body = req.getReader().lines().collect(Collectors.joining());
        String bodyHash = Base64.getEncoder()
                .encodeToString(MessageDigest.getInstance("SHA-256").digest(body.getBytes()));

        return clientId + "|" + timestamp + "|" + req.getMethod() + "|" + req.getRequestURI() + "|" + bodyHash;
    }

    private boolean verifySignature(PublicKey pub, String data, String signatureBase64) throws Exception {

        Signature sig = Signature.getInstance("SHA256withRSA");
        sig.initVerify(pub);
        sig.update(data.getBytes());
        byte[] sigBytes = Base64.getDecoder().decode(signatureBase64);
        return sig.verify(sigBytes);
    }
}
